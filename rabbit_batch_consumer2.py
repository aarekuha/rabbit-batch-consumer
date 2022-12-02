from __future__ import annotations
import json
import pika
import pickle
import threading
import functools
from queue import Queue
from queue import Empty
from typing import Any
from typing import Union
from pika.spec import Basic
from pika.spec import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from rabbit_item import RabbitItem
from exceptions import NotEnoughArgsError


class RabbitBatchConsumer(threading.Thread):
    HEARTBEAT: int = 600
    EXCHANGE_TYPE: str = "topic"

    _max_count: int
    _username: str
    _password: str
    _host: str
    _exchange: str
    _queue: str
    _port: int
    _routing_keys: list[str]
    _connection: Any
    _raw_messages_queue: Queue
    _multiple_messages_queue: Queue
    _last_delivery_tag: int

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        exchange: str,
        queue: str,
        port: int = 5672,
        routing_keys: list[str] = None,
        max_count: int = 100,
        timeout: float = 0.1,
    ) -> None:
        super().__init__()
        # Проверка наличия всех необходимых аргументов
        if not all([host, port, exchange, queue, username, password]):
            raise NotEnoughArgsError("Not enough connection params")
        # Настроки RabbitMQ
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._exchange = exchange
        self._queue = queue
        self._routing_keys = routing_keys or []
        # Настройки сервиса
        self._max_count = max_count
        self._timeout = timeout
        self._raw_messages_queue = Queue(maxsize=max_count)
        self._multiple_messages_queue = Queue()
        self._last_delivery_tag = 0

    def __iter__(self) -> RabbitBatchConsumer:
        self.daemon=True
        self.start()
        return self

    def __next__(self) -> list[RabbitItem]:
        messages: list[RabbitItem]
        while True:
            if self._last_delivery_tag:
                # Подтверждение последних обработанных сообщений
                self._connection.add_callback_threadsafe(
                    functools.partial(
                        self._channel.basic_ack,
                        self._last_delivery_tag,
                        multiple=True,
                    )
                )
                self._last_delivery_tag = 0
            try:
                messages = self._multiple_messages_queue.get(timeout=self._timeout)
                self._last_delivery_tag = messages[-1].delivery_tag
                return messages
            except Empty:
                if self._raw_messages_queue.empty():
                    continue
                messages = self._get_raw_messages_list()
                self._last_delivery_tag = messages[-1].delivery_tag
                return messages

    def _decode_message(self, body: bytes) -> Union[dict, bytes]:
        message: Union[dict, bytes]
        try:
            message = pickle.loads(body)
        except pickle.UnpicklingError:
            try:
                message = json.loads(body)
            except json.JSONDecodeError:
                message = body

        return message

    def _fill_raw_messages_queue(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes
    ) -> None:
        """ Формирование очереди с единичными сообщениями """
        self._raw_messages_queue.put(
            item=RabbitItem(
                routing_key=method.routing_key,
                message=self._decode_message(body=body),
                delivery_tag=method.delivery_tag,
            )
        )
        # Перекладывание сообщений из одиночных в групповую очередь
        if self._raw_messages_queue.qsize() == 5:
            self._multiple_messages_queue.put(item=self._get_raw_messages_list())

    def _get_raw_messages_list(self) -> list[RabbitItem]:
        """ Получение списка объектов из raw-очереди """
        messages: list[RabbitItem] = []
        while not self._raw_messages_queue.empty():
            messages.append(self._raw_messages_queue.get())
        return messages

    def run(self) -> None:
        """ Сбор в очередь одиночных сообщений """
        self._rabbit_connect()
        self._channel.basic_consume(
            queue=self._queue,
            on_message_callback=self._fill_raw_messages_queue,
        )
        self._channel.start_consuming()

    def _rabbit_connect(self) -> None:
        """ Настройка подключения к Rabbit """
        # Проверка наличия всех необходимых параметров подключения
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self._host,
                port=self._port,
                credentials=pika.PlainCredentials(
                    username=self._username,
                    password=self._password,
                ),
                heartbeat=self.HEARTBEAT,
            )
        )
        self._connection = connection
        channel = connection.channel()
        channel.queue_declare(queue=self._queue, durable=True)
        channel.exchange_declare(
            exchange=self._exchange,
            exchange_type=self.EXCHANGE_TYPE,
        )
        channel.basic_qos(prefetch_count=self._max_count)
        # Связка с масками по routing_key, при наличии
        for routing_key in self._routing_keys or []:
            channel.queue_bind(
                exchange=self._exchange,
                routing_key=routing_key,
                queue=self._queue,
            )
        self._channel = channel
        self._queue = self._queue


import contextlib

with contextlib.suppress(KeyboardInterrupt):
    for batch_messages in RabbitBatchConsumer(
        max_count=0,
        username="root",
        password="root",
        host="localhost",
        exchange="topics",
        queue="test",
        routing_keys=["test.*", "notest2"],
        timeout=3,
    ):
        print(batch_messages)
