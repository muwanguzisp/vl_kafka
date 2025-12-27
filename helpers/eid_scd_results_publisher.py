

"""
helpers/eid_scd_results_publisher.py

Publish combined EID+SCD results as ONE VL-shaped payload (Kafka-only, no Redis).

Topic : dhis2_uid
Key   : eid_scd|<infant_exp_id>|<batch_number>
Value : VL-consistent Event -> DiagnosticReport -> Observation[] (EID + SCD if available)

Identifiers:
  patient_identifier  = dbs.infant_exp_id
  specimen_identifier = b.batch_number

Window (from env, self-contained):
  BACKFILL_SINCE=YYYY-MM-DD  (default 2025-01-01)
  BACKFILL_UNTIL=YYYY-MM-DD  (default today)  [inclusive day]
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, date, timedelta, UTC
from pathlib import Path
from typing import Any, Optional, Tuple
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from kafka_producer import send_to_kafka
from helpers.fhir_utils import buildTopicFromDhis2Uid, buildMessageKey,sanitize_art_number

# ---------------- env & logging ----------------

ENV_PATH = Path.cwd() / ".env"
load_dotenv(dotenv_path=ENV_PATH)

log = logging.getLogger("eid_scd_results_publisher")
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

# DB config (EID/SCD LIMS)
DB_USER = os.getenv("EID_DB_USER", "vl_kafka")
DB_PASSWORD = os.getenv("EID_DB_PASSWORD", "..")
DB_HOST = os.getenv("EID_DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("EID_DB_PORT", "3306")
DB_NAME = os.getenv("EID_DB_NAME", "eid_lims")

# Backfill window
DEFAULT_SINCE = os.getenv("BACKFILL_SINCE", "2025-01-01")
DEFAULT_UNTIL = os.getenv("BACKFILL_UNTIL", datetime.now(UTC).date().isoformat())  # inclusive date


def _engine() -> Engine:
    host = "127.0.0.1" if DB_HOST.strip().lower() == "localhost" else DB_HOST.strip()
    user = quote_plus(DB_USER)
    pwd = quote_plus(DB_PASSWORD)
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{DB_PORT}/{DB_NAME}"
    log.info("[EID DB] %s:***@%s:%s/%s", DB_USER, host, DB_PORT, DB_NAME)
    return create_engine(url, pool_pre_ping=True, future=True)


SQL = text("""
SELECT DISTINCT
    dbs.id,
    b.batch_number                    AS specimen_identifier,   -- batch_number
    dbs.infant_exp_id                 AS patient_identifier,    -- infant_exp_id

    dbs.accepted_result               AS eid_result,
    dbs.pcr                           AS eid_pcr,
    dbs.non_routine                   AS non_routine_pcr,
    dbs.date_dbs_tested               AS eid_date_tested,
    dbs.sample_verified_on            AS eid_date_approved,

    dbs.SCD_test_result               AS scd_result,
    b.date_SCD_testing_completed      AS scd_date_tested,

    b.date_rcvd_by_cphl               AS date_received_at_cphl,

    b.qc_done,
    b.scd_qc_done,

    f.dhis2_uid,
    b.facility_id
FROM batches b
LEFT JOIN dbs_samples dbs ON dbs.batch_id = b.id
LEFT JOIN facilities f    ON b.facility_id = f.id
WHERE DATE(b.date_rcvd_by_cphl) >= :since
  AND DATE(b.date_rcvd_by_cphl) <=  :until_exclusive
  AND (
        (b.qc_done='YES'     AND dbs.accepted_result IS NOT NULL)
     OR (b.scd_qc_done='YES' AND dbs.SCD_test_result  IS NOT NULL)
  )
ORDER BY b.date_rcvd_by_cphl ASC, dbs.id ASC
""")


# ---------------- helpers ----------------

def _iso(dt: Any) -> Optional[str]:
    if not dt:
        return None
    if isinstance(dt, (datetime, date)):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _until_exclusive(until_inclusive: str) -> str:
    # inclusive YYYY-MM-DD  ->  exclusive YYYY-MM-DD by adding 1 day
    d = _parse_date(until_inclusive)
    return (d + timedelta(days=1)).isoformat()


def _build_key(patient_id: str, specimen_id: str) -> bytes | str:
    """
    Prefer your shared buildMessageKey if it supports a program argument.
    Otherwise fallback to plain prefixed string.
    """
    try:
        return buildMessageKey(patient_id, specimen_id, program="eid_scd")
    except TypeError:
        # older signature: buildMessageKey(patient, specimen)
        return f"eid_scd|{patient_id}|{specimen_id}"


def _build_combined_vl_shaped_payload(row) -> dict:
    """
    Build one payload consistent with VL structure:
    Event -> DiagnosticReport -> Observation[]
    """
    dhis2_uid = str(row["dhis2_uid"])
    patient_id = str(row["patient_identifier"])
    specimen_id = str(row["specimen_identifier"])

    eid_available = (row.get("qc_done") == "YES") and bool(row.get("eid_result"))
    scd_available = (row.get("scd_qc_done") == "YES") and bool(row.get("scd_result"))

    observations = []

    if eid_available:
        observations.append({
            "resourceType": "Observation",
            "effectiveDateTime": _iso(row.get("eid_date_tested")),
            "valueString": str(row.get("eid_result")),
            # optional extra fields (safe if EMR ignores)
            "code": "202512401",
            "details": {
                "pcr": row.get("eid_pcr"),
                "non_routine_pcr": row.get("non_routine_pcr"),
                "released_at": _iso(row.get("eid_date_approved")),
            },
        })

    if scd_available:
        observations.append({
            "resourceType": "Observation",
            "effectiveDateTime": _iso(row.get("scd_date_tested")),
            "valueString": str(row.get("scd_result")),
            "code": "202512402",
        })

    # issue timestamp: prefer latest available (SCD tested time; else EID approved; else EID tested)
    issued_at = (
        _iso(row.get("scd_date_tested"))
        or _iso(row.get("eid_date_tested"))
    )

    payload = {
        "resourceType": "Event",
        "status": "completed",
        "subject": {
            "resourceType": "Location",
            "name": dhis2_uid
        },
        "reasonReference": {
            "resourceType": "DiagnosticReport",
            # Keep "final" for compatibility with existing VL readers
            "status": "final",
            # Program/panel code (EMR can still read Observations without caring about this)
            "code": "202512300",
            "subject": {
                "resourceType": "Patient",
                "identifier": patient_id
            },
            "result": observations,
            "specimen": [
                {
                    "resourceType": "Specimen",
                    "identifier": specimen_id
                }
            ],
            "issue": issued_at,
            "performer": {
                "resourceType": "Organization",
                "identifier": "CPHL",
                "name": "Central Public Health Laboratory"
            }
        }
    }

    return payload, dhis2_uid, patient_id, specimen_id, eid_available, scd_available


# ---------------- public API ----------------

def publish_eid_scd_results() -> dict:
    """
    Self-contained publisher (no args passed in).
    Reads window + DB config from environment.
    Returns {rows, sent, errors}.
    """
    since = DEFAULT_SINCE
    until_inclusive = DEFAULT_UNTIL
    until_excl = _until_exclusive(until_inclusive)

    log.info("EID/SCD publish window: since=%s until<=%s (SQL uses < %s)", since, until_inclusive, until_excl)

    eng = _engine()
    rows = 0
    sent = 0
    errors = 0

    with eng.connect() as conn:
        res = conn.execute(SQL, {"since": since, "until_exclusive": until_excl})

        for row in res.mappings():
            rows += 1
            try:
                dhis2_uid = row.get("dhis2_uid")
                if not dhis2_uid:
                    raise ValueError("Missing dhis2_uid (cannot route to topic)")

                payload, dhis2_uid, patient_id, specimen_id, eid_ok, scd_ok = _build_combined_vl_shaped_payload(row)

                # only publish if at least one result exists
                if not (eid_ok or scd_ok):
                    continue

                topic = buildTopicFromDhis2Uid(dhis2_uid)
                sanitized_patient_id = sanitize_art_number(patient_id)
                key = _build_key(sanitized_patient_id, specimen_id)

                meta = send_to_kafka(
                    topic=topic,
                    payload=payload,
                    key=key,
                    headers=[
                        ("program", b"eid_scd"),
                        ("message-type", b"result"),  # keep same as VL
                        ("patient_identifier", patient_id.encode()),
                        ("specimen_identifier", specimen_id.encode()),
                    ],
                )
                sent += 1

                if sent % 500 == 0:
                    log.info("EID/SCD ...sent=%d (last @ %s/%s)", sent, meta["partition"], meta["offset"])

            except Exception as e:
                errors += 1
                log.error(
                    "EID/SCD publish failed dbs.id=%s patient=%s specimen=%s : %s",
                    row.get("id"),
                    row.get("patient_identifier"),
                    row.get("specimen_identifier"),
                    e,
                )

    log.info("EID/SCD done. rows=%d sent=%d errors=%d", rows, sent, errors)
    return {"rows": rows, "sent": sent, "errors": errors}

