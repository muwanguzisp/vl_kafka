from kafka import KafkaProducer

KAFKA_SERVER = '216.104.204.152:9092'  # Replace with your actual IP
TOPIC = 'vl_hie_test_request'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

message = b'Hello Kafka from Vagrant Python!'
producer.send(TOPIC, message)
producer.flush()

print("✅ Message sent successfully.")
