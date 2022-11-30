import time
import pika
import json
import pickle
import threading
from typing import Any
from typing import Union
from typing import Callable
from pika.spec import Basic
from pika.adapters.blocking_connection import BlockingChannel
from json.decoder import JSONDecodeError

from exceptions import CallbackNotDefinedError
from exceptions import NotEnoughArgsError
from rabbit_item import RabbitItem

CallableType = Union[list[RabbitItem], Any]

EXCHANGE_TYPE: str = "topic"
PREFETCH_COUNT: int = 100
HEARTBEAT: int = 600


class RabbitBatchConsumer:
    """
    Батчевый консьюмер RabbitMQ
    Подключение к очереди, сбор данных в буффер для порционирования
        сообщений.
        - при достаточном количестве сообщений в очереди
          (max_count), вызывает callback-функцию с подготовленными
          данными
        - если данные есть, но их меньше, то callback-функция
          вызывается по timeout'у
        - если данные отсутствуют, то callback-функция не вызывается

    Args:
        max_count: int - batch size of items
        timeout: float - waiting collection of max_count items
        callback: Callable(list[RabbitItem]) - callback on items collected
                                               by max_count or timeout
        host: str - RabbitMQ host
        port: int - RabbitMQ port (default is 5672)
        username, password: str - RabbitMQ credentials
        exchange, queue: str - RabbitMQ topic
        routing_keys: list[str] - RabbitMQ routing keys, topic filter

    Raises:
      CallbackNotDefinedError: callback not defined
      NotEnoughArgsError: not enough connection args
          Mandatory arguments:
            host: str
            username: str
            password: str
            exchange: str
            queue: str

    Example:
    >>>

    from contextlib import suppress

    with suppress(KeyboardInterrupt):
        def worker_func(items: list[RabbitItem]) -> None:
            print(f"{len(items)}")
            for item in items:
                print(f"{item.routing_key=}, {item.message=}")

        RabbitBatchConsumer(
            max_count=5,
            callback=worker_func,
            username="root",
            password="root",
            host="localhost",
            exchange="topics",
            queue="test",
            routing_keys=["test.*", "notest2"],
            timeout=3,
        ).run()

    """
    _max_count: int
    _buffer: CallableType
    _callback: Callable[[CallableType], None]
    _channel: BlockingChannel
    _queue: str

    def __init__(
        self,
        max_count: int,
        callback: Callable[[CallableType], None],
        username: str,
        password: str,
        host: str,
        exchange: str,
        queue: str,
        port: int = 5672,
        routing_keys: list[str] = None,
        timeout: float = 0.1,
    ) -> None:
        # callback-функция должна быть передана (она вызывается, при
        #   получении данных из очереди)
        if not callback or not isinstance(callback, Callable):
            raise CallbackNotDefinedError("Callback must be declared")
        # Проверка наличия всех необходимых параметров подключения
        if host and port and exchange and queue and username and password:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    port=port,
                    credentials=pika.PlainCredentials(
                        username=username,
                        password=password,
                    ),
                    heartbeat=HEARTBEAT,
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue=queue, durable=True)
            channel.exchange_declare(
                exchange=exchange,
                exchange_type=EXCHANGE_TYPE,
            )
            channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            # Связка с масками по routing_key
            for routing_key in routing_keys or []:
                channel.queue_bind(
                    exchange=exchange,
                    routing_key=routing_key,
                    queue=queue,
                )
            self._channel = channel
            self._queue = queue
        else:
            raise NotEnoughArgsError("Not enough connection params")
        self._max_count = max_count
        self._buffer = []
        self._callback = callback
        # Запуск процесса мониторинга достижения timeout'а и вызова
        #   callback'а до достижения лимита сообщений в буфере
        self._timeout_watcher_thread = threading.Thread(
            target=self._timeout_watcher,
            kwargs={
                "timeout": timeout,
                "callback": self._callback,
                "buffer": self._buffer,
            },
        )
        self._timeout_watcher_thread.daemon = True
        self._timeout_watcher_thread.start()
        self.ack = self._channel.basic_ack

    def _timeout_watcher(
        self,
        timeout: float,
        callback: Callable[[CallableType], None],
        buffer: CallableType,
    ) -> None:
        """
        Вызов Callback'а по timeout'у
        timeout: время задержки до принудительного вызова callback'а,
                 при наличии данных
        callback: функция принимающая список подготовленных данных (list[RabbitItem])
        """
        while True:
            time.sleep(timeout)
            if buffer:
                callback(buffer)
                buffer.clear()

    def run(self) -> None:
        """
        Сбор данных из RabbitMQ и их размещение в промежуточном буфере, для последующей
            доставки в callback-функцию по timeout'у или при достижении максимального
            объема (max_count)
        """
        body: Union[bytes, None]
        method: Union[Basic.Deliver, None]
        for method, _, body in self._channel.consume(self._queue, auto_ack=True):
            if not body or not method:
                continue
            # Попытка десериализации полученных данных (тип RabbitItem.message)
            message: Union[dict, bytes]
            try:
                message = pickle.loads(body)
            except pickle.UnpicklingError:
                try:
                    message = json.loads(body)
                except JSONDecodeError:
                    message = body
            # Добавление записи в буфер
            self._buffer.append(
                RabbitItem(
                    routing_key=method.routing_key,
                    message=message,
                )
            )
            # Проверка достижения лимита сообщений в буфере до вызова callback'а
            if len(self._buffer) >= self._max_count:
                self._callback(self._buffer[:self._max_count])
                self._buffer.clear()
