#!/usr/bin/env python3
"""
Publish released VL results to Kafka (return leg, no Redis).

- Topic      : dhis2_uid
- Key        : patient_identifier|specimen_identifier
- Payload    : matches provided Event/DiagnosticReport/Observation shape
- Window     : --since (default 2025-01-01) to --until (default: today)
"""

import os, json, logging, argparse
from datetime import datetime, date,UTC

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from kafka_producer import send_to_kafka                  # already in your repo
from helpers.fhir_utils import buildTopicFromDhis2Uid, buildMessageKey  # small helper file

# ---------- config & logging ----------

load_dotenv()
log = logging.getLogger("return_producer")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DB_USER = os.getenv("DB_USER", "homestead")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "vl_lims")

def _engine() -> Engine:
    # use PyMySQL driver
    url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True, future=True)

# ---------- SQL (released results only) ----------

SQL = text("""
SELECT
    s.id                        AS sample_id,
    r.id                        AS result_id,
    r.test_date                 AS test_date,
    r.result_numeric            AS result_numeric,
    r.result_alphanumeric       AS result_alphanumeric,
    s.form_number               AS specimen_identifier,
    p.art_number                AS art_number,
    p.other_id                  AS other_id,
    s.facility_reference        AS facility_reference,
    qc.released_at              AS released_at,
    qc.released                 AS released,
    r.suppressed                AS suppressed,
    f.id                        AS facility_id,
    p.facility_id               AS patient_facility_id,
    f.dhis2_uid                 AS dhis2_uid
FROM vl_samples s
JOIN vl_results r         ON r.sample_id = s.id
LEFT JOIN vl_results_qc qc ON qc.result_id = r.id
LEFT JOIN vl_patients p    ON s.patient_id = p.id
LEFT JOIN backend_facilities f ON f.id = p.facility_id
WHERE qc.released = 1
  AND r.test_date >= :since
  AND r.test_date <  :until_plus_1
ORDER BY r.test_date ASC, s.id ASC
""")

# ---------- helpers ----------

def _iso(dt) -> str | None:
    if not dt:
        return None
    if isinstance(dt, (datetime, date)):
        # match your sample "YYYY-MM-DD HH:MM:SS"
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)

def _choose_patient_identifier(art_number, other_id) -> str:
    # prefer ART number; fall back to other_id; else use sample_id as last resort
    if art_number and str(art_number).strip():
        return str(art_number).strip()
    if other_id and str(other_id).strip():
        return str(other_id).strip()

def _build_payload(row) -> dict:
    """
    Build the exact JSON you provided.
    """
    dhis2_uid            = row["dhis2_uid"]
    specimen_identifier  = row["facility_reference"]
    patient_identifier   = row["art_number"]
    test_date            = _iso(row["test_date"])
    released_at          = _iso(row["released_at"])

    # result fields
    value_int  = row["result_numeric"]
    value_str  = row["result_alphanumeric"]
    # Ensure string types as in your sample
    value_int_s = f"{value_int:.2f}" if value_int is not None else None

    payload = {
        "resourceType": "Event",
        "status": "completed",
        "subject": {  # Location is the facility/dhis2 UID
            "resourceType": "Location",
            "name": dhis2_uid
        },
        "reasonReference": {
            "resourceType": "DiagnosticReport",
            "status": "final",
            "code": "315124004",  # Viral load, per your example
            "subject": {
                "resourceType": "Patient",
                "identifier": str(patient_identifier)
            },
            "result": [
                {
                    "resourceType": "Observation",
                    "effectiveDateTime": test_date,                   # test_date
                    "valueInteger": value_int_s,                      # "50.00"
                    "valueString": value_str                          # "< 50.00 Copies / mL"
                }
            ],
            "specimen": [
                {
                    "resourceType": "Specimen",
                    "identifier": str(specimen_identifier)
                }
            ],
            "issue": released_at,  # release timestamp
            "performer": {
                "resourceType": "Organization",
                "identifier": "CPHL",
                "name": "Central Public Health Laboratory"
            }
        }
    }

    return payload, dhis2_uid, patient_identifier, specimen_identifier

# ---------- main run ----------

def main():
    parser = argparse.ArgumentParser(description="Backfill/publish released VL results to Kafka.")
    parser.add_argument("--since", default=os.getenv("BACKFILL_SINCE", "2025-01-01"),
                        help="Inclusive start date (YYYY-MM-DD), default 2025-01-01")
    parser.add_argument("--until",default=os.getenv("BACKFILL_UNTIL", datetime.now(UTC).date().isoformat()),
        help="Exclusive end date (YYYY-MM-DD), default = today")
    
    args = parser.parse_args()

    since = args.since
    until = args.until

 

    log.info("Publishing released results window: since=%s until<%s", since, until)

    eng = _engine()
    rows = 0
    sent = 0
    errors = 0

    with eng.connect() as conn:
        res = conn.execute(SQL, {"since": since, "until_plus_1": until})
        for row in res.mappings():
            rows += 1
            try:
                payload, dhis2_uid, patient_id, specimen_id = _build_payload(row)

                topic = buildTopicFromDhis2Uid(dhis2_uid)
                key   = buildMessageKey(str(patient_id), str(specimen_id))

                meta = send_to_kafka(
                    topic=topic,
                    payload=payload,
                    key=key,
                    headers=[
                        ("patient_identifier", str(patient_id).encode()),
                        ("specimen_identifier", str(specimen_id).encode()),
                        ("message-type", b"result"),
                    ],
                )
                sent += 1
                if sent % 500 == 0:
                    log.info("...sent so far: %d (last @ %s/%s)", sent, meta["partition"], meta["offset"])
            except Exception as e:
                errors += 1
                log.error("Failed to publish sample_id=%s result_id=%s: %s",
                          row.get("sample_id"), row.get("result_id"), e)

    log.info("Done. rows=%d sent=%d errors=%d", rows, sent, errors)

if __name__ == "__main__":
    main()
