# kafka_producer.py
import os, json
from kafka import KafkaProducer
from dotenv import load_dotenv
from helpers.fhir_utils import _buildKafkaSecurityOptions

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "216.104.204.152:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    acks=os.getenv("KAFKA_ACKS", "all"),
    retries=int(os.getenv("KAFKA_RETRIES", "5")),
    linger_ms=int(os.getenv("KAFKA_LINGER_MS", "0")),
    batch_size=int(os.getenv("KAFKA_BATCH_BYTES", "16384")),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    **_buildKafkaSecurityOptions(),
)


#def send_to_kafka(topic: str, payload: dict) -> dict:
#    """Send and wait for broker ack; returns partition/offset."""
#    fut = producer.send(topic, payload)
#    meta = fut.get(timeout=15)
#    return {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}

def send_to_kafka(
    topic: str,
    payload: dict,
    key: bytes | str | None = None,
    headers: list[tuple[str, bytes]] | None = None,
    timeout: int = 15,
) -> dict:
    """
    Send a message and wait for broker ack. Returns topic/partition/offset.
    - key: str or bytes (Kafka message key)
    - headers: list of (str, bytes)
    """
    key_bytes = key.encode("utf-8") if isinstance(key, str) else key
    futureRecordMetaDataInstance = producer.send(topic, value=payload, key=key_bytes, headers=headers or [])
    meta = futureRecordMetaDataInstance.get(timeout=timeout)
    return {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}

