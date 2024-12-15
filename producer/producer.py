import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "kafka:9092"  # Internal service name for Kafka in Docker
TOPIC = "example_topic"

# Possible actions for simulated client activity
ACTIONS = ["login", "logout", "purchase", "click"]

time.sleep(15)  # Wait for 10 seconds

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Kafka Producer started. Sending messages to 'example_topic'...")

try:
    # Infinite loop to continuously send messages
    while True:
        message = {
            "user_id": random.randint(1, 10),  # Random user IDs
            "action": random.choice(ACTIONS),  # Random action
            "timestamp": datetime.now().isoformat()  # Current timestamp
        }

        # Send message to Kafka
        producer.send(TOPIC, message)
        print(f"Sent: {message}")

        time.sleep(1)  # Simulate 1-second delay between messages

except KeyboardInterrupt:
    print("Kafka Producer stopped.")
finally:
    producer.close()
