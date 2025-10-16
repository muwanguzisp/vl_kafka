# vl_kafka_consumer.py
import json, os, signal, sys, traceback
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy.orm import sessionmaker
from db import engine
from openhie_vl import SessionOpenHIE as OpenHieSessionLocal
from validator import validate_vl_payload_thoroughly
from helpers.fhir_utils import insert_patient_data, insert_sample_data,log_incomplete_data,_buildKafkaSecurityOptions 
import logging
import time

# Load .env file
from dotenv import load_dotenv
load_dotenv()

# ---------- Configuration ----------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "216.104.204.152:9092")
REQUEST_TOPIC = os.environ.get("VL_REQUEST_TOPIC", "vl_single_payload_request")

CONSUMER_GROUP  = os.environ.get("VL_CONSUMER_GROUP", "vl_lims_consumer_group")
AUTO_OFFSET     = os.environ.get("VL_AUTO_OFFSET", "earliest")  # earliest|latest

# ---------- Logging ----------
logger = logging.getLogger("vl_kafka_consumer")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)

# ---------- Graceful shutdown ----------
_shutdown = False
def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Shutdown signal received. Stopping consumer...")
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

def main():
    logger.info(f"Starting consumer on {KAFKA_BOOTSTRAP} topic='{REQUEST_TOPIC}', group='{CONSUMER_GROUP}'")

    consumer = KafkaConsumer(
        REQUEST_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset=AUTO_OFFSET,
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
        max_poll_records=50,
        **_buildKafkaSecurityOptions(),
    )

    SessionLocal = sessionmaker(bind=engine)
    processed = inserted = failed = 0
    last_heartbeat = 0.0
    
    logger.info("Consumer started and polling...")
    try:
        while not _shutdown:
            batch = consumer.poll(timeout_ms=1000, max_records=50)
           
            if not batch:
                now = time.time()
                if now - last_heartbeat > 5:  # every 5s
                    logger.info("...no messages yet; still polling")
                    last_heartbeat = now
                continue

            for tp, records in batch.items():
                for rec in records:
                    processed += 1
                    try:
                        payload = rec.value
                        logger.info(f"Received message offset={rec.offset} partition={tp.partition}")

                        # 1) Use one session to validate + insert
                        with SessionLocal() as session:
                            # ⚠️ Validator is expected to resolve facility_id ONCE using the session
                            logger.info(f"....c1....")
                            validated = validate_vl_payload_thoroughly(payload, session=session)

                            facility_id = validated.get("facility_id")
                            if not facility_id:
                                raise ValueError("Validator did not return facility_id")

                            patient_data = validated.get("patient", {})
                            sample_data  = validated.get("sample", {})

                            # 2) Insert/update
                            logger.info(f"....c2....")
                            patient_id = insert_patient_data(session, patient_data, facility_id)
                            insert_sample_data(session, sample_data, facility_id, patient_id)

                            logger.info(f"....c3....")
                            inserted += 1
                            logger.info(f"✅ Inserted/updated patient & sample (offset={rec.offset})")

                            print(f"Checking for errors:....")
                            print(validated)
                            if validated.get("errors"):
                                with OpenHieSessionLocal() as oh_session:
                                    log_incomplete_data(
                                        oh_session,
                                        specimen_id=validated["sample"].get("form_number"),
                                        patient_id=validated["patient"].get("art_number"),
                                        facility_id=validated.get("facility_id"),
                                        errors=validated.get("errors"),       # dict/list/str/None all OK
                                        payload=payload,                       # original bundle
                                        merge=True,                            # or False to overwrite
                                    )
                                    logger.warning(f"❌ Incomplete specimen {validated['sample'].get('form_number')} logged.")
                            

                    except Exception as e:
                        failed += 1
                        try:
                            snippet = json.dumps(rec.value)[:400]
                        except Exception:
                            snippet = "<payload unprintable>"
                        logger.error(f"❌ Failure at offset={rec.offset}: {e}. Payload: {snippet}")
                        logger.debug(traceback.format_exc())

    except KafkaError as kerr:
        logger.error(f"Kafka error: {kerr}")
    except Exception as exc:
        logger.error(f"Unhandled consumer exception: {exc}")
        logger.debug(traceback.format_exc())
    finally:
        try:
            consumer.close(timeout=5)
        except Exception:
            pass
        logger.info(f"Consumer shut down. Stats => processed={processed}, inserted={inserted}, failed={failed}")

if __name__ == "__main__":
    main()
