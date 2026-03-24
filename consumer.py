import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "bet-consumer-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

# 🔥 updated topic
consumer.subscribe(["bets"])

print("Bet Consumer started. Waiting for messages...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
        else:
            bet = json.loads(msg.value().decode("utf-8"))

            print(f"\n🎯 New Bet Received:")
            print(f"Bet ID   : {bet['bet_id']}")
            print(f"User     : {bet['user_id']}")
            print(f"Match    : {bet['match']}")
            print(f"Team     : {bet['team']}")
            print(f"Odds     : {bet['odds']}")
            print(f"Stake    : ₹{bet['stake']}")
            print(f"Time     : {bet['timestamp']}")

finally:
    consumer.close()
