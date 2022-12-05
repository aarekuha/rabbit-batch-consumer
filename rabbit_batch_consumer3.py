from __future__ import annotations
import json
import pika
import pickle
from typing import Any
from typing import Union
from typing import Generator
import pika.compat as compat
from pika.adapters.blocking_connection import _QueueConsumerGeneratorInfo
from pika.adapters.blocking_connection import _ConsumerCancellationEvt

from rabbit_item import RabbitItem
from exceptions import NotEnoughArgsError


class RabbitBatchConsumer:
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
    _buffer: list[RabbitItem]

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
        self._buffer = []
        self._rabbit_connect()

    def __iter__(self) -> Generator[list[RabbitItem], None, None]:
        channel = self._channel
        channel._impl._raise_if_not_open()
        params = (self._queue, None, None)
        if channel._queue_consumer_generator is not None:
            if params != channel._queue_consumer_generator.params:
                raise ValueError(
                    'Consume with different params not allowed on existing '
                    'queue consumer generator; previous params: %r; '
                    'new params: %r' % (
                        channel._queue_consumer_generator.params,
                        (self._queue, None, None)
                    )
                )
        else:
            consumer_tag = channel._impl._generate_consumer_tag()
            channel._queue_consumer_generator = _QueueConsumerGeneratorInfo(params, consumer_tag)
            try:
                channel._basic_consume_impl(
                    queue=self._queue,
                    auto_ack=None,
                    exclusive=None,
                    consumer_tag=consumer_tag,
                    arguments=None,
                    alternate_event_sink=channel._on_consumer_generator_event)
            except Exception:
                channel._queue_consumer_generator = None
                raise
        wait_start_time = compat.time_now()
        wait_deadline = wait_start_time + self._timeout
        delta = self._timeout
        while channel._queue_consumer_generator is not None:
            # Process pending events
            if channel._queue_consumer_generator.pending_events:
                evt = channel._queue_consumer_generator.pending_events.popleft()
                if type(evt) is _ConsumerCancellationEvt:  # pylint: disable=C0123
                    # Consumer was cancelled by broker
                    channel._queue_consumer_generator = None
                    break
                else:
                    self._buffer.append(
                        RabbitItem(
                            routing_key=evt.method.routing_key,
                            message=self._decode_message(body=evt.body),
                            delivery_tag=evt.method.delivery_tag,
                        )
                    )
                    delta = wait_deadline - compat.time_now()
                    if len(self._buffer) >= self._max_count:
                        yield self._buffer
                        channel.basic_ack(delivery_tag=self._buffer[-1].delivery_tag, multiple=True)
                        self._buffer = []
                        wait_start_time = compat.time_now()
                        wait_deadline = wait_start_time + self._timeout
                        delta = self._timeout
                    continue
            if self._timeout is None:
                channel._process_data_events(time_limit=None)
                continue
            while (channel._queue_consumer_generator is not None and
                   not channel._queue_consumer_generator.pending_events):
                if delta > 0:
                    channel._process_data_events(time_limit=delta)
                    if not channel._queue_consumer_generator:
                        break
                    if channel._queue_consumer_generator.pending_events:
                        break
                    delta = wait_deadline - compat.time_now()
                elif delta <= 0.0:
                    if self._buffer:
                        yield self._buffer
                        channel.basic_ack(delivery_tag=self._buffer[-1].delivery_tag, multiple=True)
                        self._buffer = []
                    wait_start_time = compat.time_now()
                    wait_deadline = wait_start_time + self._timeout
                    delta = self._timeout
                    break

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


for list_of_items in RabbitBatchConsumer(
    host="localhost",
    username="root",
    password="root",
    exchange="topics",
    queue="test",
    timeout=3,
):
    print(len(list_of_items))
