import json
import logging
import os
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaException

from db import get_session  # your existing SQLAlchemy session factory
from helpers.fhir_utils import (
    get_lims_facility_id,
    build_legacy_patient_data_from_bio,
    build_legacy_sample_data_from_bio,
    build_legacy_patient_data_from_program,
    build_legacy_sample_data_from_program,
    upsert_legacy_patient_data,
    upsert_legacy_sample_data,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] legacy_consumer - %(message)s",
)


# ---------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------

def _get_dhis2_uid_from_bio_payload(payload: Dict[str, Any]) -> Optional[str]:
    """
    Legacy BIO (ServiceRequest):
      DHIS2 UID / facility code is in payload.locationCode
    """
    return payload.get("locationCode")


def _get_dhis2_uid_from_program_payload(payload: Dict[str, Any]) -> Optional[str]:
    """
    Legacy PROGRAM (Observation):
      DHIS2 UID is in contained.Patient.managingOrganization.reference
    """
    for res in payload.get("contained", []):
        if res.get("resourceType") == "Patient":
            org = res.get("managingOrganization") or {}
            return org.get("reference")
    return None


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------

def handle_service_request(session, payload: Dict[str, Any]) -> None:
    """
    Handle legacy BIO_DATA (ServiceRequest) payload:

    - Resolve facility_id from locationCode (DHIS2 UID).
    - Build patient_data + sample_data using helpers.
    - Upsert into vl_patients + vl_samples.
    """
    dhis2_uid = _get_dhis2_uid_from_bio_payload(payload)
    if not dhis2_uid:
        logger.warning("BIO payload missing locationCode (DHIS2 UID); skipping.")
        return

    facility_id = get_lims_facility_id(session, dhis2_uid)
    if not facility_id:
        logger.warning("BIO payload DHIS2 UID %s not mapped to any LIMS facility; skipping.", dhis2_uid)
        return

    patient_data = build_legacy_patient_data_from_bio(payload)
    sample_data = build_legacy_sample_data_from_bio(payload)

    logger.info(
        "BIO_DATA: facility_id=%s, art_number=%s, specimen=%s",
        facility_id,
        patient_data.get("art_number"),
        sample_data.get("form_number"),
    )

    patient_id = upsert_legacy_patient_data(session, patient_data, facility_id)
    upsert_legacy_sample_data(session, sample_data, facility_id, patient_id)


def handle_observation(session, payload: Dict[str, Any]) -> None:
    """
    Handle legacy PROGRAM_DATA (Observation) payload:

    - Resolve facility_id from contained.Patient.managingOrganization.reference.
    - Build patient_data + sample_data using helpers.
    - Upsert into vl_patients + vl_samples.
    """
    dhis2_uid = _get_dhis2_uid_from_program_payload(payload)
    if not dhis2_uid:
        logger.warning("PROGRAM payload missing DHIS2 UID (managingOrganization.reference); skipping.")
        return

    facility_id = get_lims_facility_id(session, dhis2_uid)
    if not facility_id:
        logger.warning("PROGRAM payload DHIS2 UID %s not mapped to any LIMS facility; skipping.", dhis2_uid)
        return

    patient_data = build_legacy_patient_data_from_program(payload)
    sample_data = build_legacy_sample_data_from_program(payload)

    logger.info(
        "PROGRAM_DATA: facility_id=%s, art_number=%s, specimen=%s",
        facility_id,
        patient_data.get("art_number"),
        sample_data.get("form_number"),
    )

    patient_id = upsert_legacy_patient_data(session, patient_data, facility_id)
    upsert_legacy_sample_data(session, sample_data, facility_id, patient_id)


# ---------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------

def run_legacy_consumer() -> None:
    """
    Legacy VL consumer:

    - Subscribes to VL_LEGACY_REQUEST_TOPIC
    - Reads raw FHIR payloads (ServiceRequest / Observation)
    - Dispatches to the appropriate handler
    - Upserts into VL LIMS database.
    """

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP")
    if not bootstrap_servers:
        raise RuntimeError("KAFKA_BOOTSTRAP is not set in the environment")

    topic = os.getenv("VL_REQUEST_TOPIC", "vl_legacy_payload_request")
    group_id = os.getenv("VL_LEGACY_CONSUMER_GROUP", "vl_legacy_lims_consumer")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    logger.info("Starting legacy VL consumer: topic=%s, group_id=%s", topic, group_id)

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value())
            except json.JSONDecodeError as e:
                logger.exception("Failed to decode JSON payload: %s", e)
                # commit so we don't get stuck on a bad message
                consumer.commit(msg)
                continue

            rtype = (payload.get("resourceType") or "").strip()

            session = get_session()
            try:
                if rtype == "ServiceRequest":
                    handle_service_request(session, payload)
                elif rtype == "Observation":
                    handle_observation(session, payload)
                else:
                    logger.warning(
                        "Unknown or missing resourceType in legacy payload: %r; skipping.",
                        rtype,
                    )
        
                session.commit()
                               
                # Mark message as processed
                consumer.commit(msg)
                logger.info(
                    "Successfully processed legacy payload: resourceType=%s, offset=%s",
                    rtype,
                    msg.offset(),
                )

            except Exception as e:
                session.rollback()
                                
                # You can decide: rethrow (no commit) if you want retry,
                # or commit (skip) if you want to avoid blocking.
                logger.exception("Error processing legacy payload: %s", e)
                # For now, commit to skip the problematic message:
                consumer.commit(msg)
            finally:
                session.close()
    finally:
        logger.info("Shutting down legacy VL consumer")
        consumer.close()


if __name__ == "__main__":
    run_legacy_consumer()
