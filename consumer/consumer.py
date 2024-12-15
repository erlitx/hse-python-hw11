import json
import time
from collections import defaultdict
from kafka import KafkaConsumer


time.sleep(15)  # Wait for 10 seconds

# Kafka consumer setup
consumer = KafkaConsumer(
    "example_topic",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id="group1"
)

# Storage for counting actions
user_actions = defaultdict(lambda: defaultdict(int))

print("Consumer started, listening for messages...")

try:
    for message in consumer:
        data = message.value
        user_id = data["user_id"]
        action = data["action"]

        # Update action count
        user_actions[user_id][action] += 1

        print(f"Received: {data}")
        print("\nTop Users by Actions:")
        for user, actions in sorted(user_actions.items(), key=lambda x: sum(x[1].values()), reverse=True):
            print(f"User {user}: {dict(actions)}")
except KeyboardInterrupt:
    print("Consumer stopped.")
