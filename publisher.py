import time
import pika
import json


HOST: str = "localhost"
USERNAME: str = "root"
PASSWORD: str = "root"

EXCHANGE: str = "topics"
EXCHANGE_TYPE: str = "topic"
QUEUE: str = "test"
TEST_MESSAGES_COUNT: int = 123
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
    # time.sleep(MESSAGE_SEND_DELAY)
    body = {
        "attr1": str(i * 4),
        "attr2": str(i * 2),
    }
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=f"{QUEUE}.{i}",
        body=json.dumps(body),
    )
    if not i % 10_000:
        print(f"Published ({QUEUE}.{i}): {body}")
