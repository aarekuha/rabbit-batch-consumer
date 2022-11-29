import time
import pika


HOST: str = "localhost"
USERNAME: str = "root"
PASSWORD: str = "root"

EXCHANGE: str = "topics"
EXCHANGE_TYPE: str = "topic"
QUEUE: str = "test"
TEST_MESSAGES_COUNT: int = 7
MESSAGE_SEND_DELAY: float = 0.01

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=HOST,
        credentials=pika.PlainCredentials(
            username=USERNAME,
            password=PASSWORD,
        ),
    )
)
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE)
channel.queue_declare(queue=QUEUE, durable=True)
channel.queue_bind(exchange=EXCHANGE, queue=QUEUE)

for i in range(TEST_MESSAGES_COUNT):
    time.sleep(MESSAGE_SEND_DELAY)
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=QUEUE,
        body=str(i),
    )
    print(f"Published: {i}")
