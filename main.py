from fastapi import FastAPI, Request, HTTPException,Depends
from kafka_producer import send_to_kafka
from validator import validate_vl_payload_mini
from helpers.fhir_response_utils import generate_fhir_response
from helpers.fhir_utils import sanitize_art_number
from datetime import datetime
from fastapi.responses import JSONResponse
from security_basic_db import createBasicAuthWithApiTokenDependency


from dotenv import load_dotenv

from pydantic import BaseModel, field_validator
from typing import List, Literal, Optional
from kafka import KafkaProducer
import redis, json, time, uuid, os

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()
#app = FastAPI()

# ---------------- Config ----------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "216.104.204.152:9092")

VL_REQUEST_TOPIC = os.getenv("VL_REQUEST_TOPIC", "vl_single_payload_request")
VL_RESULTS_TOPIC = os.getenv("VL_RESULTS_TOPIC", "results-request-topic")

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

#---------security configurations -----------
FORWARD_CLIENT_ID     = os.getenv("FORWARD_CLIENT_ID")
FORWARD_CLIENT_SECRET = os.getenv("FORWARD_CLIENT_SECRET")

RETURN_CLIENT_ID      = os.getenv("RETURN_CLIENT_ID")
RETURN_CLIENT_SECRET  = os.getenv("RETURN_CLIENT_SECRET")

requireForwardAuth = createBasicAuthWithApiTokenDependency(FORWARD_CLIENT_ID, FORWARD_CLIENT_SECRET)
requireReturnAuth  = createBasicAuthWithApiTokenDependency(RETURN_CLIENT_ID,  RETURN_CLIENT_SECRET)

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

@app.post("/single_payload",dependencies=[Depends(requireForwardAuth)])
async def post_vl_request(request: Request):

    payload = await request.json()
    try:
        validated_data = validate_vl_payload_mini(payload)

        # ✅ Send to Kafka
        send_to_kafka(VL_REQUEST_TOPIC, payload)   # forward leg ✅
        

        # ✅ FHIR success response
        fhir_response = generate_fhir_response(
            status="ok",
            narrative="Bio data and program data successfully captured",
            data={
                "time_stamp": datetime.now().isoformat(),
                "patient_identifier": validated_data["patient"]["art_number"],
                "specimen_identifier": validated_data["sample"]["form_number"],
                "lims_sample_id": ""  # Optional or filled in consumer
            }
        )
        return JSONResponse(status_code=200, content=fhir_response)

    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content=e.detail)

    except Exception as e:
        # fallback internal error
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal error: {str(e)}"}
        )


@app.post("/sample_result", dependencies=[Depends(requireReturnAuth)])
async def vl_results(payload: ServiceRequestIn):
    # extract fields from your exact input shape
    location_code        = payload.locationCode
    specimen_identifier  = payload.specimen[0].identifier
    art_number           = payload.specimen[0].subject.identifier
    sanitized_art_number = sanitize_art_number(art_number)

    key = cache_key(location_code, specimen_identifier, sanitized_art_number)

    # 1) cache fast-path
    cached = _redis_get_json(key)
    if cached:
        return cached

    # 2) produce to Kafka
    request_id = str(uuid.uuid4())
    producer.send(
        VL_RESULTS_TOPIC,
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