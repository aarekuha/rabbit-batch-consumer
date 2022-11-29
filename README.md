#### Rabbit batch consumer
##### Задача
Подключение к очереди и коллекционирование данных до достижения выбранного размера одной пачки или до достижения
указанного времени. В случае, когда данные присутствуют, но они не собраны в указанном количестве, доставляются в подписанную функцию
(callback) в имеющемся количестве по истечении указанного времени.
##### Зависимости
- pika
##### Пример использования
```python
    from contextlib import suppress

    from .rabbit_batch_consumer import RabbitBatchConsumer

    with suppress(KeyboardInterrupt):
        def worker_func(items: list[dict]) -> None:
            print(f"{items=}")

        RabbitBatchConsumer(
            max_count=5,
            timeout=3,
            callback=worker_func,
            host="localhost",
            exchange="topics",
            queue="test",
            username="root",
            password="root",
        ).run()
```
