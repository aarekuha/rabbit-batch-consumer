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

    from rabbit_batch_consumer import RabbitBatchConsumer
    from rabbit_item import RabbitItem

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
```
