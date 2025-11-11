# results_consumer.py
from kafka import KafkaConsumer
from pathlib import Path
import mysql.connector, redis, json, os
from datetime import datetime
from helpers.fhir_utils import sanitize_art_number,_buildKafkaSecurityOptions
from decimal import Decimal
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ENV_PATH = Path.cwd() / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# ------------- Config -------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "216.104.204.152:9092")
VL_RESULTS_TOPIC = os.getenv("VL_RESULTS_TOPIC", "results-request-topic")
VL_RESULTS_CONSUMER_GROUP     = os.getenv("VL_RESULTS_CONSUMER_GROUP", "vl-results-consumer")

MYSQL_HOST = os.getenv("MYSQL_HOST", "192.168.1.144")
MYSQL_USER = os.getenv("MYSQL_USER", "homestead")
MYSQL_PASS = os.getenv("MYSQL_PASS", "secret")
MYSQL_DB   = os.getenv("MYSQL_DB",   "openhie_vl_20250312")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))
CACHE_TTL  = int(os.getenv("CACHE_TTL", "1800"))  # 30 minutes

AUTO_OFFSET = os.getenv("VL_AUTO_OFFSET", "earliest")

# ------------- Clients -------------
consumer = KafkaConsumer(
    VL_RESULTS_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id=VL_RESULTS_CONSUMER_GROUP,
    **_buildKafkaSecurityOptions(),
)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

print(f"[cfg] KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP}")
print(f"[cfg] TOPIC={VL_RESULTS_TOPIC} GROUP={VL_RESULTS_CONSUMER_GROUP} AUTO_OFFSET={AUTO_OFFSET}")
print(f"[cfg] MYSQL_HOST={MYSQL_HOST}:{MYSQL_PORT} DB={MYSQL_DB} USER={MYSQL_USER}")
print(f"[cfg] REDIS={REDIS_HOST}:{REDIS_PORT}/{REDIS_DB} TTL={CACHE_TTL}s")



db = mysql.connector.connect(
    host=MYSQL_HOST,port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB
)
cursor = db.cursor(dictionary=True)

# ------------- Helpers -------------
def cache_key(location_code: str, specimen_identifier: str, art_number: str) -> str:
    return f"vl_result:{location_code}:{specimen_identifier}:{art_number}"

def to_iso(dt) -> str:
    if not dt:
        return None
    if isinstance(dt, (datetime,)):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    # assume already a string (e.g., "2025-07-17 13:18:56")
    return str(dt)

# Map DB row to Event/completed (your exact shape)
def build_event_completed(row, location_code: str, art_number: str, specimen_identifier: str) -> dict:
    """
    Expected columns in `row`:
      - viral_load_copies (numeric)
      - result_status (e.g., "Target Not Detected", "Suppressed", "Not Suppressed")
      - date_tested, date_collected
      - facility_id (optional for you)
    """
    # Observation values:
    # if copies is 0 (or None but status says TND), put "0.00" & "Target Not Detected"
    # else copies as integer string & a status string
    vl_copies = row.get("viral_load_copies")
    status    = row.get("result_status") or ""

    vl_result_numeric = row.get("result_numeric")
    if isinstance(vl_result_numeric, Decimal):
        vl_result_numeric = str(vl_result_numeric)
    

    return {
        "resourceType": "Event",
        "status": "completed",
        "subject": {
            "resourceType": "Location",
            "name": location_code
        },
        "reasonReference": {
            "resourceType": "DiagnosticReport",
            "status": "final",
            "code": "315124004",  # (kept from your example)
            "subject": {
                "resourceType": "Patient",
                "identifier": art_number
            },
            "result": [
                {
                    "resourceType": "Observation",
                    "effectiveDateTime": to_iso(row.get("test_date")),
                    "valueInteger": vl_result_numeric,
                    "valueString": row.get("result_alphanumeric")
                }
            ],
            "specimen": [
                {
                    "resourceType": "Specimen",
                    "identifier": specimen_identifier
                }
            ],
            "issue": to_iso(row.get("date_released")),
            "performer": {
                "resourceType": "Organization",
                "identifier": "CPHL",
                "name": "Central Public Health Laboratory"
            }
        }
    }

# NOTE: Adapt this SQL to your schema
SQL = """
SELECT
    sample_id,test_date,result_numeric,result_alphanumeric,form_number,art_number,other_id,
    date_released,released,facility_id,patient_facility_id,dhis2_uid 
    
FROM vl_sample_results 
WHERE dhis2_uid = %s
  AND facility_reference = %s
  AND sanitized_art_number = %s 
LIMIT 1
"""

# ------------- Main loop -------------
for msg in consumer:
    try:
        data = msg.value
        print(f"data type: {data.get('type')}")
        if data.get("type") != "results_query":
            continue
        

        location_code       = data["location_code"]
        specimen_identifier = data["specimen_identifier"]
        art_number          = data.get("art_number")  # may be None/empty
        sanitized_art_number = sanitize_art_number(art_number)

        key = cache_key(location_code, specimen_identifier, sanitized_art_number or "")

        cursor.execute(SQL, (location_code, specimen_identifier, sanitized_art_number))
        row = cursor.fetchone()

        print ("The sql query for results: ")
        print(SQL)
        print(f"Parameters; dhis2 uid: {location_code},specimen id: {specimen_identifier}, art_number: {art_number}")

        if row and row.get("test_date"):
            # Build final Event/completed
            event = build_event_completed(
                row,
                location_code=location_code,
                art_number=art_number or row.get("art_number"),
                specimen_identifier=specimen_identifier
            )
            print ("build_event_completed returned successfully : ")
            r.setex(key, CACHE_TTL, json.dumps(event))
            print ("results published...: ")
        else:
            # If not tested yet (missing row or no date_tested), do NOT write a final result.
            # Let API return Event/pending until lab posts a result.
            # Optional: you could write a short-lived "pending" cache to reduce DB churn.
            pass

    except Exception as e:
        print(f"[consumer] error: {e}")
        continue
