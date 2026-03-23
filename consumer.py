import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

print("Consumer started. Waiting for messages...")
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
        else:
            order = json.loads(msg.value().decode("utf-8"))
            print(f"Received: {order}")
finally:
    consumer.close()
