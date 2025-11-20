from kafka import KafkaProducer
import json
import time

def create_producer():
    for _ in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            print("Kafka Producer connected")
            return producer
        except Exception as e:
            print("Kafka not ready... retrying", e)
            time.sleep(2)

    raise Exception("Failed to connect to Kafka")