# results_api.py
from fastapi import FastAPI
from pydantic import BaseModel, field_validator
from typing import List, Literal, Optional
from kafka import KafkaProducer
import redis, json, time, uuid, os

# ---------------- Config ----------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "216.104.204.152:9092")
KAFKA_TOPIC_REQ = os.getenv("KAFKA_TOPIC_REQ", "results-request-topic")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))
WAIT_LOOPS = int(os.getenv("WAIT_LOOPS", "10"))   # 10 x 0.5s = ~5s
WAIT_SLEEP = float(os.getenv("WAIT_SLEEP", "0.5"))

# ---------------- Payload Schemas (incoming) ----------------
class PatientSubject(BaseModel):
    resourceType: Literal["Patient"]
    identifier: str   # ART number

class SpecimenItem(BaseModel):
    resourceType: Literal["Specimen"]
    identifier: str   # barcode/form number
    subject: PatientSubject

class SubjectLocation(BaseModel):
    resourceType: Literal["Location"]
    name: str         # source system name (e.g., JCRC_LIMS)

class ServiceRequestIn(BaseModel):
    resourceType: Literal["ServiceRequest"]
    locationCode: str                 # DHIS2 UID
    subject: SubjectLocation
    specimen: List[SpecimenItem]

    @field_validator("specimen")
    @classmethod
    def at_least_one_specimen(cls, v):
        if not v:
            raise ValueError("specimen must have at least one item")
        return v

# ---------------- App & clients ----------------
app = FastAPI(title="VL Results API", version="4.0.1")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# ---------------- Helpers ----------------
def cache_key(location_code: str, specimen_identifier: str, art_number: Optional[str]) -> str:
    suffix = f":{art_number}" if art_number else ""
    return f"vl_result:{location_code}:{specimen_identifier}{suffix}"

def _redis_get_json(key: str):
    b = r.get(key)
    if not b:
        return None
    # Redis returns bytes -> decode to str before json.loads
    if isinstance(b, bytes):
        b = b.decode("utf-8")
    return json.loads(b)

def event_pending(location_code: str, specimen_identifier: str, art_number: str) -> dict:
    # exact “pending” envelope you requested
    return {
        "resourceType": "Event",
        "status": "pending",
        "subject": {"resourceType": "Location", "name": location_code},
        "reasonReference": {
            "resourceType": "DiagnosticReport",
            "status": "pending",
            "subject": {"resourceType": "Patient", "identifier": art_number},
            "result": [],
            "specimen": [{"resourceType": "Specimen", "identifier": specimen_identifier}],
            "issue": "",
            "performer": {"resourceType": "Organization", "identifier": "", "name": ""},
        },
    }

# ---------------- Endpoint ----------------
@app.post("/api/v1/vl_results")
async def vl_results(payload: ServiceRequestIn):
    # extract fields from your exact input shape
    location_code        = payload.locationCode
    specimen_identifier  = payload.specimen[0].identifier
    art_number           = payload.specimen[0].subject.identifier

    key = cache_key(location_code, specimen_identifier, art_number)

    # 1) cache fast-path
    cached = _redis_get_json(key)
    if cached:
        return cached

    # 2) produce to Kafka
    request_id = str(uuid.uuid4())
    producer.send(
        KAFKA_TOPIC_REQ,
        {
            "type": "results_query",
            "request_id": request_id,
            "location_code": location_code,
            "specimen_identifier": specimen_identifier,
            "art_number": art_number,
            "source_system": payload.subject.name,
        },
    )

    # 3) wait briefly for consumer to cache result
    for _ in range(WAIT_LOOPS):
        cached = _redis_get_json(key)
        if cached:
            return cached
        time.sleep(WAIT_SLEEP)

    # 4) still not ready -> return Event/pending (your shape)
    return event_pending(location_code, specimen_identifier, art_number)
