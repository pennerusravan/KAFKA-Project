import uuid
import json
import time
from confluent_kafka import Producer

producer_config = {"bootstrap.servers": "kafka:9092"}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()}")

print("Betting Producer started...")

while True:
    bet = {
        "bet_id": str(uuid.uuid4()),
        "user_id": "user1",
        "match": "IND vs AUS",
        "bet_type": "win",
        "team": "IND",
        "odds": 1.85,
        "stake": 500,
        "timestamp": time.time()
    }

    producer.produce(
        topic="bets",
        key=bet["bet_id"].encode("utf-8"),
        value=json.dumps(bet).encode("utf-8"),
        callback=delivery_report
    )

    producer.flush()

    print(f"Sent: {bet}")
    time.sleep(3)
