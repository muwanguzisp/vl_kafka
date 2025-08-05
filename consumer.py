from kafka import KafkaConsumer

KAFKA_SERVER = '216.104.204.152:9092'
TOPIC = 'vl_hie_test_request'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='vagrant-consumer'
)

print(f"🟢 Listening for messages on '{TOPIC}'...")

for message in consumer:
    print(f"📩 Received: {message.value.decode('utf-8')}")
