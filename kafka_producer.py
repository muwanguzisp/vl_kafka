import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='216.104.204.152:9092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

def send_to_kafka(topic, payload):
    producer.send(topic, payload)
    producer.flush()