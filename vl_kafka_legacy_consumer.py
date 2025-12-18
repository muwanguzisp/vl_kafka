import json
import logging
import os
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaException
from datetime import datetime,date
from db import get_session, get_eid_session
from models.EidBatch import EidBatch
from models.EidDbsSample import EidDbsSample

from helpers.fhir_utils import (
    get_lims_facility_id,
    build_legacy_patient_data_from_bio,
    build_legacy_sample_data_from_bio,
    build_legacy_patient_data_from_program,
    build_legacy_sample_data_from_program,
    upsert_legacy_patient_data,
    upsert_legacy_sample_data,

    extract_eid_data_from_bundle,
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

from datetime import datetime
from sqlalchemy import func
from models.EidBatch import EidBatch
from models.EidDbsSample import EidDbsSample
from fastapi import HTTPException
import logging

logger = logging.getLogger("legacy_consumer")


def handleEidScdBundle(session, bundle):
    """
    Handles incoming EID (or Sickle Cell) FHIR Bundles.

    Logic:
      1. Extract data from FHIR bundle (already done in fhir_utils).
      2. Use facility_id from extract_eid_data_from_bundle().
      3. Find or create the batch.
      4. Check for existing DBS sample by (facility_id, exp_number, batch_number).
      5. If found and same child → update.
      6. If found and different child → skip (do not insert).
      7. If not found → insert new record.
    """
    try:
        eid_data = extract_eid_data_from_bundle(session, bundle)
        batch_data = eid_data["batch"]
        sample_data = eid_data["sample"]

        # === Step 1: Extract core identifiers ===
        facility_id = batch_data.get("facility_id")
        facility_name = batch_data.get("facility_name")
        facility_district = batch_data.get("facility_district")
        batch_number = batch_data.get("batch_number")
        exp_number = sample_data.get("exp_number")

        if not all([facility_id, batch_number, exp_number]):
            raise HTTPException(
                status_code=422,
                detail="Missing key identifiers: facility_id, batch_number, or exp_number."
            )

        # === Step 2: Find or create batch ===
        batch = session.query(EidBatch).filter_by(batch_number=batch_number).first()
        if batch:
            logger.info(f"Reusing existing EID batch #{batch.batch_number} (id={batch.id})")
        else:
            batch = EidBatch(
                lab=batch_data.get("lab", "CPHL"),
                batch_number=batch_number,
                facility_id=facility_id,
                facility_name=facility_name,
                facility_district=facility_district,
                requesting_unit="EID",
                results_return_address="Via EMR HIE",
                results_transport_method="POSTA_UGANDA",
                date_rcvd_by_cphl=batch_data.get("date_rcvd_by_cphl"),
                date_entered_in_DB=datetime.now().date(),
                all_samples_rejected="NO",
                PCR_results_released="NO",
                SCD_results_released="NO",
                qc_done="NO",
                scd_qc_done="NO",
                rejects_qc_done="NO",
                f_paediatricART_available="NO",
                tests_requested=batch_data.get("tests_requested", "PCR"),
                senders_comments=batch_data.get("senders_comments"),
                source_system=4
            )
            session.add(batch)
            session.flush()
            logger.info(f"Created new EID batch #{batch.batch_number} for facility {facility_name}.")

        # === Step 3: Check for existing sample by composite key ===
        existing_sample = (
            session.query(EidDbsSample)
            .join(EidBatch, EidBatch.id == EidDbsSample.batch_id)
            .filter(
                EidBatch.facility_id == facility_id,
                EidDbsSample.infant_exp_id == exp_number,
                EidBatch.batch_number == batch_number
            )
            .first()
        )

        # === Step 4: Update / skip / insert ===
        if existing_sample:
            if existing_sample.infant_exp_id == exp_number:
                logger.info(
                    f"Updating existing EID sample for exp_number={exp_number}, batch={batch_number}, facility={facility_name}."
                )
                existing_sample.infant_name = sample_data.get("infant_name")
                existing_sample.infant_gender = sample_data.get("infant_gender", "NOT_RECORDED")
                existing_sample.infant_dob = sample_data.get("infant_dob")
                existing_sample.date_dbs_taken = sample_data.get("date_dbs_taken")
                existing_sample.infant_feeding = sample_data.get("infant_feeding")
                existing_sample.given_contri = sample_data.get("given_contri", "BLANK")
                existing_sample.updated_at = datetime.now()
            else:
                logger.warning(
                    f"Skipping new sample: batch={batch_number} already assigned to another child "
                    f"({existing_sample.infant_exp_id} ≠ {exp_number}) at {facility_name}."
                )
                return  # ✅ Skip conflicting insert
        else:
            dbs_sample = EidDbsSample(
                batch_id=batch.id,
                pos_in_batch=1,
                infant_name=sample_data.get("infant_name"),
                infant_exp_id=exp_number,
                infant_gender=sample_data.get("infant_gender", "NOT_RECORDED"),
                infant_dob=sample_data.get("infant_dob"),
                date_dbs_taken=sample_data.get("date_dbs_taken"),
                sample_rejected="NOT_YET_CHECKED",
                infant_feeding=sample_data.get("infant_feeding"),
                given_contri=sample_data.get("given_contri", "BLANK"),
                test_type=sample_data.get("test_type", "P"),
                PCR_test_requested="YES",
                SCD_test_requested="NO",
                date_data_entered=datetime.now().date()
            )
            session.add(dbs_sample)
            logger.info(
                f"Inserted new EID sample for exp_number={exp_number}, batch={batch_number}, facility={facility_name}."
            )

        session.commit()
        logger.info(f"EID bundle processed successfully for batch {batch_number}.")
        return

    except Exception as e:
        session.rollback()
        logger.exception(f"Error processing EID bundle: {e}")
        raise


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

            vl_session = get_session()

            try:
                # --- Detect FHIR EID Bundle ---
                if rtype == "Bundle" and payload.get("type", "").lower() == "transaction":
                    for entry in payload.get("entry", []):
                        res = entry.get("resource", {})
                        if res.get("resourceType") == "ServiceRequest":
                            for c in res.get("code", {}).get("coding", []):
                                system = str(c.get("system", "")).lower()
                                code = str(c.get("code", "")).lower()
                                if "hmis.health.go.ug" in system and code == "acp_014":
                                    logger.info("EID bundle detected; routing to EID LIMS.")
                                    eid_session = get_eid_session()
                                    handleEidScdBundle(eid_session, payload)
                                    eid_session.commit()
                                    eid_session.close()
                                    break

                # --- Fallback: legacy (VL) handling ---
                elif rtype == "ServiceRequest":
                    handle_service_request(vl_session, payload)
                elif rtype == "Observation":
                    handle_observation(vl_session, payload)
                else:
                    logger.warning(
                        "Unknown or missing resourceType in legacy payload: %r; skipping.",
                        rtype,
                    )
        
                vl_session.commit()
                               
                # Mark message as processed
                consumer.commit(msg)
                logger.info(
                    "Successfully processed legacy payload: resourceType=%s, offset=%s",
                    rtype,
                    msg.offset(),
                )

            except Exception as e:
                vl_session.rollback()
                                
                # You can decide: rethrow (no commit) if you want retry,
                # or commit (skip) if you want to avoid blocking.
                logger.exception("Error processing legacy payload: %s", e)
                # For now, commit to skip the problematic message:
                consumer.commit(msg)
            finally:
                vl_session.close()
    finally:
        logger.info("Shutting down legacy VL consumer")
        consumer.close()


if __name__ == "__main__":
    run_legacy_consumer()
