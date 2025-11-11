import mysql.connector
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy import text
from models import LimsLabTech,LimsClinician,LimsPatient, LimsSample, IncompleteDataLog
import os
from dotenv import load_dotenv

import hashlib,json

import re
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()

def get_sample_type_from_bundle_element(entry_element):
    sample_type_flag = None

    type_section = entry_element.get("resource", {}).get("type", {})
    codings = type_section.get("coding", [])

    for coding in codings:
        system = coding.get("system", "").lower()
        code = coding.get("code", "").lower()
        if "unhls" in system:
            if "202501023" in code:
                sample_type_flag = "P"  # Plasma
                break
            elif "202501022" in code:
                sample_type_flag = "D"  # DBS
                break

    return sample_type_flag

def generate_name(name_array):
    if not name_array:
        return None
    name = name_array[0]
    return name.get("text") or " ".join(name.get("given", [])) + " " + name.get("family", "")

def generate_phone(telecom_array):
    for contact in telecom_array:
        if contact.get("system") == "phone":
            return contact.get("value")
    return None

def get_lab_tech_id_from_reference(reference, payload, facility_id, db_config):
    technician_id = reference.split("/")[-1]
    lab_tech_id = None

    for entry in payload.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Practitioner" and resource.get("id") == technician_id:
            name = generate_name(resource.get("name", []))
            phone = generate_phone(resource.get("telecom", []))

            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor(dictionary=True)

            query = "SELECT * FROM vl_lab_techs WHERE facility_id = %s AND lname LIKE %s"
            cursor.execute(query, (facility_id, name))
            result = cursor.fetchone()

            if result:
                lab_tech_id = result["id"]
            else:
                insert_query = """
                    INSERT INTO vl_lab_techs (lname, lphone, facility_id)
                    VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (name, phone, facility_id))
                conn.commit()
                lab_tech_id = cursor.lastrowid

            cursor.close()
            conn.close()

    return lab_tech_id




def is_specimen_identifier_valid(specimen_identifier: str) -> bool:
    this_year = datetime.now().strftime("%y")  # e.g., "25"
    previous_year = (datetime.now().replace(year=datetime.now().year - 1)).strftime("%y")  # e.g., "24"

    specimen_identifier = str(specimen_identifier).strip()

    pattern = rf"^({this_year}|{previous_year})"
    match = re.match(pattern, specimen_identifier)

    is_valid = match is not None
    logger.info(f"Specimen ID: {specimen_identifier}, Pattern: {pattern}, Valid: {'yes' if is_valid else 'no'}")

    return is_valid

def generate_name(name_elements: list) -> str:
    """
    Build full name from FHIR name element.
    """
    full_name = ""
    for value in name_elements:
        if "given" in value:
            full_name += " " + " ".join(value["given"])
        if "family" in value:
            full_name += " " + value["family"]
            break
    return full_name.strip()

def generate_phone_contact(telecom_elements: list) -> str:
    """
    Extract phone number from FHIR telecom element.
    """
    for instance in telecom_elements:
        if instance.get("system") == "phone":
            return instance.get("value", "")
    return ""

def get_lab_technician_id_from_entry_element(reference: str, payload: dict, selected_facility_id: int, session: Session):
    """
    Lookup or create a lab technician in vl_lab_techs based on Practitioner in FHIR payload.
    """
    technician_number = reference[13:] if reference and len(reference) > 13 else None
    lab_technician_id = None

    for entry_element in payload.get("entry", []):
        resource = entry_element.get("resource", {})
        if (
            resource.get("resourceType") == "Practitioner"
            and resource.get("id") == technician_number
            and selected_facility_id > 0
        ):
            lab_contact_person_name = generate_name(resource.get("name", []))
            lab_contact_person_phone = generate_phone_contact(resource.get("telecom", []))

            # Check if lab tech exists in DB
            existing = (
                session.query(LimsLabTech)
                .filter(
                    LimsLabTech.facility_id == selected_facility_id,
                    LimsLabTech.lname.ilike(f"%{lab_contact_person_name}%")
                )
                .first()
            )

            if existing:
                lab_technician_id = existing.id
            else:
                new_lab_tech = LimsLabTech(
                    lname=lab_contact_person_name,
                    lphone=lab_contact_person_phone,
                    facility_id=selected_facility_id
                )
                session.add(new_lab_tech)
                session.commit()
                lab_technician_id = new_lab_tech.id

    return lab_technician_id

def get_lims_facility_id(session, dhis2_uid: str):
    sql = text("""
        SELECT id
        FROM backend_facilities
        WHERE lower(dhis2_uid) = lower(:uid)
        LIMIT 1
    """)
    row = session.execute(sql, {"uid": dhis2_uid}).fetchone()
    return int(row[0]) if row else None

def get_clinician_id_from_reference(reference: str, payload: dict, facility_id: int, session: Session):
    """
    Lookup or create a clinician in vl_clinicians based on Practitioner in FHIR payload.
    """
    if not reference or not _gt_zero(facility_id):
        return None

    # Extract practitioner ID from reference string (after "Practitioner/")
    if reference.startswith("Practitioner/"):
        practitioner_id = reference.split("/")[-1]
    else:
        practitioner_id = reference

    clinician_id = None

    for entry in payload.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Practitioner" and resource.get("id") == practitioner_id:
            clinician_name = generate_name(resource.get("name", []))
            clinician_phone = generate_phone_contact(resource.get("telecom", []))

            # Check if clinician already exists in DB
            existing = (
                session.query(LimsClinician)
                .filter(
                    LimsClinician.facility_id == facility_id,
                    LimsClinician.cname.ilike(f"%{clinician_name}%")
                )
                .first()
            )

            if existing:
                clinician_id = existing.id
            else:
                new_clinician = LimsClinician(
                    cname=clinician_name,
                    cphone=clinician_phone,
                    facility_id=facility_id
                )
                session.add(new_clinician)
                session.commit()
                clinician_id = new_clinician.id

            break  # Found matching Practitioner, stop looping

    return clinician_id

def sanitize_art_number(art_number: str) -> str:
    """
    Remove all non-alphanumeric characters from ART number.
    Equivalent to PHP preg_replace('/[^a-zA-Z0-9]/', '', $art_number).
    """
    if not art_number:
        return ""
    return re.sub(r'[^a-zA-Z0-9]', '', art_number)


def generate_unique_id(facility_id: int, art_number: str) -> str:
    """
    Generate unique ID in the format: facility_id-A-ARTNUMBER.
    """
    sanitized_art = sanitize_art_number(art_number)
    return f"{facility_id}-A-{sanitized_art}"

def get_who_clinical_stage(snomed_code: str) -> int | None:
    """
    Map SNOMED code -> WHO clinical stage (1..4).
    Returns None if unknown.
    """
    if not snomed_code:
        return None

    code = str(snomed_code).strip()
    if code in ("737378009", "103415007"):
        return 1
    elif code in ("737379001", "103416008"):
        return 2
    elif code in ("737380003", "103417004"):
        return 3
    elif code in ("737381004", "103418009"):
        return 4
    return None

def get_who_clinical_stage_from_element(entry_element: dict) -> int | None:
    """
    Read WHO stage from an Observation entry if:
      - code.coding contains system ~ 'snomed' AND code == '385354005'
      - then read valueCodeableConcept.coding[*] where system ~ 'snomed'
      - map that code via get_who_clinical_stage(...)
    """
    resource = entry_element.get("resource", {})
    code_block = resource.get("code", {}) or {}
    codings = code_block.get("coding", []) or []

    for coding in codings:
        system = (coding.get("system") or "").lower()
        code = coding.get("code")
        if "snomed" in system and code == "385354005":
            for vc in resource.get("valueCodeableConcept", {}).get("coding", []) or []:
                v_system = (vc.get("system") or "").lower()
                v_code = vc.get("code")
                if "snomed" in v_system and v_code:
                    return get_who_clinical_stage(v_code)
            # If we got here, structure didn’t have a valid SNOMED code in valueCodeableConcept
            return None

    return None

from datetime import datetime

def _normalize_date_yyyy_mm_dd(dt_str: str) -> str | None:
    """Return YYYY-MM-DD if parseable, else None."""
    if not dt_str:
        return None
    # Try a few common formats; fallback to fromisoformat
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(dt_str, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def get_art_initiation_date(entry_element: dict) -> str | None:
    """
    Return ART initiation date as YYYY-MM-DD if the Observation matches:
      - CIEL code 9860155 with system containing 'CIEL'
      - SNOMED code 413946009 with system containing 'SNOMED'
    and has valueDateTime.
    """
    valid_code_to_system_keyword = {
        "9860155": "ciel",
        "413946009": "snomed",
    }

    resource = entry_element.get("resource", {})
    codings = (resource.get("code", {}) or {}).get("coding", []) or []
    if not codings:
        return None

    for coding in codings:
        code = coding.get("code")
        system = (coding.get("system") or "")
        if not code or not system:
            continue

        expected_keyword = valid_code_to_system_keyword.get(str(code))
        if expected_keyword and expected_keyword in system.lower():
            val_dt = resource.get("valueDateTime")
            return _normalize_date_yyyy_mm_dd(val_dt)

    return None

def _lc(val):
    return (val or "").lower()


def _value_code_by_system(resource: dict, system_contains: str) -> str | None:
    """
    Look inside resource['valueCodeableConcept']['coding'][]. Return the first code
    whose system contains `system_contains` (case-insensitive).
    """
    vcc = (resource or {}).get("valueCodeableConcept", {}) or {}
    for c in vcc.get("coding", []) or []:
        if system_contains in _lc(c.get("system")):
            return c.get("code")
    return None


def get_treatment_line_id_from_element(entry_element: dict) -> int | None:
    """
    If Observation has code system ~ 'cphl' and code '202501016',
    read valueCodeableConcept.coding (system ~ 'cphl') and map:
        202501017 -> 89
        202501018 -> 90
        202501019 -> 215
    """
    resource = (entry_element or {}).get("resource", {}) or {}
    codings = (resource.get("code", {}) or {}).get("coding", []) or []

    for coding in codings:
        if "cphl" in _lc(coding.get("system")) and coding.get("code") == "202501016":
            selected_code = _value_code_by_system(resource, "cphl")
            if selected_code == "202501017":
                return 89
            elif selected_code == "202501018":
                return 90
            elif selected_code == "202501019":
                return 215
            return None
    return None


def get_treatment_indication_id_from_element(entry_element: dict) -> int | None:
    """
    If Observation has code system ~ 'cphl' and code '202501009',
    read valueCodeableConcept.coding (system ~ 'cphl') and map to LIMS IDs:
        202501010 -> 93   (Routine Monitoring)
        202501011 -> 94   (Repeat VL)
        202501012 -> 95   (Suspected Treatment Failure)
        202501013 -> 97   (6 months after ART initiation)
        202501025 -> 98   (12 months after ART initiation)
        202501024 -> 99   (Repeat after IAC)
        202501014 -> 100  (1st ANC For PMTCT)
        202501015 -> 101  (CCLAD Entry)
    """
    resource = (entry_element or {}).get("resource", {}) or {}
    codings = (resource.get("code", {}) or {}).get("coding", []) or []

    mapping = {
        "202501010": 93,
        "202501011": 94,
        "202501012": 95,
        "202501013": 97,
        "202501025": 98,
        "202501024": 99,
        "202501014": 100,
        "202501015": 101,
    }

    for coding in codings:
        if "cphl" in _lc(coding.get("system")) and coding.get("code") == "202501009":
            selected_code = _value_code_by_system(resource, "cphl")
            return mapping.get(selected_code)
    return None


def get_adherence_id_from_element(entry_element: dict) -> int | None:
    """
    If Observation has code system ~ 'loinc' and code 'LL5723-3',
    read valueCodeableConcept.coding (system ~ 'loinc') and map:
        LA8967-7 -> 1
        LA8968-5 -> 2
        LA8969-3 -> 3
    (case-insensitive comparison for safety)
    """
    resource = (entry_element or {}).get("resource", {}) or {}
    codings = (resource.get("code", {}) or {}).get("coding", []) or []

    loinc_code = "ll5723-3"
    mapping = {
        "la8967-7": 1,
        "la8968-5": 2,
        "la8969-3": 3,
    }

    for coding in codings:
        if "loinc" in _lc(coding.get("system")) and _lc(coding.get("code")) == loinc_code:
            selected_code = _value_code_by_system(resource, "loinc")
            return mapping.get(_lc(selected_code or ""))
    return None

def get_treatment_care_approach(code: str) -> int | None:
    """
    Map treatment care approach codes or mnemonics to LIMS IDs.

    Args:
        code (str): Code string (e.g. 'fbim', '202501004').

    Returns:
        int | None: LIMS ID if found, otherwise None.
    """
    if not code:
        return None

    code = str(code).strip().lower()

    mapping = {
        "fbim": 1,
        "202501004": 1,
        "fbg": 2,
        "202501005": 2,
        "ftdr": 3,
        "202501006": 3,
        "cddp": 4,
        "202501007": 4,
        "cclad": 5,
        "202501008": 5,
    }

    return mapping.get(code, None)




def insert_patient_data(session: Session, patient_data: dict, facility_id: int) -> int:
    """
    Insert or update patient record in vl_patients.
    Returns patient_id.
    """
    art_number = patient_data.get("art_number")
    if not art_number:
        raise ValueError("Missing ART number for patient")

    sanitized_art = sanitize_art_number(art_number)

    # Check if patient already exists (unique by sanitized_art + facility_id)
    existing_patient = (
        session.query(LimsPatient)
        .filter(
            LimsPatient.sanitized_art_number == sanitized_art,
            LimsPatient.facility_id == facility_id,
        )
        .first()
    )

    if existing_patient:
        # Update existing if needed
        existing_patient.dob = patient_data.get("dob", existing_patient.dob)
        existing_patient.gender = patient_data.get("gender", existing_patient.gender)
        existing_patient.treatment_initiation_date = patient_data.get(
            "treatment_initiation_date", existing_patient.treatment_initiation_date
        )
        session.commit()
        return existing_patient.id
    else:
        # Create new
        new_patient = LimsPatient(
            unique_id=generate_unique_id(facility_id, art_number),
            art_number=art_number,
            sanitized_art_number=sanitized_art,
            dob=patient_data.get("dob"),
            gender=patient_data.get("gender"),
            facility_id=facility_id,
            treatment_initiation_date=patient_data.get("treatment_initiation_date"),
            created_by_id=1  # default system user
        )
        session.add(new_patient)
        session.commit()
        return new_patient.id


def insert_sample_data(session: Session, sample_data: dict, facility_id: int, patient_id: int) -> int:
    """
    Insert or update sample record in vl_samples.
    Returns sample_id.
    """
    form_number = sample_data.get("form_number")
    if not form_number:
        raise ValueError("Missing form_number (specimen id) for sample")

    # Check if sample already exists (unique by facility_reference)
    existing_sample = (
        session.query(LimsSample)
        .filter(LimsSample.facility_reference == form_number)
        .first()
    )

    if existing_sample:
        # Update existing
        existing_sample.date_collected = sample_data.get("date_collected", existing_sample.date_collected)
        existing_sample.sample_type = sample_data.get("sample_type", existing_sample.sample_type)
        existing_sample.treatment_indication_id = sample_data.get("treatment_indication_id", existing_sample.treatment_indication_id)
        existing_sample.treatment_line_id = sample_data.get("treatment_line_id", existing_sample.treatment_line_id)
        existing_sample.arv_adherence_id = sample_data.get("arv_adherence_id", existing_sample.arv_adherence_id)
        existing_sample.current_who_stage = sample_data.get("current_who_stage", existing_sample.current_who_stage)
        existing_sample.lab_tech_id = sample_data.get("lab_tech_id", existing_sample.lab_tech_id)
        existing_sample.clinician_id = sample_data.get("clinician_id", existing_sample.clinician_id)
        existing_sample.facility_id = facility_id
        existing_sample.patient_id = patient_id
        session.commit()
        return existing_sample.id
    else:
        # Create new
        new_sample = LimsSample(
            form_number=form_number,
            facility_reference=form_number,
            date_collected=sample_data.get("date_collected"),
            sample_type=sample_data.get("sample_type"),
            treatment_indication_id=sample_data.get("treatment_indication_id"),
            treatment_line_id=sample_data.get("treatment_line_id"),
            arv_adherence_id=sample_data.get("arv_adherence_id"),
            current_who_stage=sample_data.get("current_who_stage"),
            lab_tech_id=sample_data.get("lab_tech_id"),
            clinician_id=sample_data.get("clinician_id"),
            facility_id=facility_id,
            patient_id=patient_id,
            source_system=sample_data.get("source_system"),
        )
        session.add(new_sample)
        session.commit()
        return new_sample.id

def get_gender_flag(gender_string):
    """
    Map free-text gender to a single-letter flag.
    Returns: 'F' for female, 'M' for male, 'X' for unknown/other.
    """
    if not gender_string:
        return "X"
    cleaned = str(gender_string).strip().lower()
    if cleaned in ("female", "f"):
        return "F"
    if cleaned in ("male", "m"):
        return "M"
    return "X"



import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from models.IncompleteDataLog import IncompleteDataLog

logger = logging.getLogger(__name__)


def _sanitize_id(value: Optional[str]) -> str:
    """Trim and coalesce falsy IDs to 'unknown'."""
    if not value:
        return "unknown"
    return " ".join(str(value).strip().split()) or "unknown"


def _hash_payload(payload: Optional[dict]) -> str:
    """Stable SHA256 of the JSON payload (sorted keys, compact separators)."""
    payload_norm = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload_norm.encode("utf-8")).hexdigest()


def _canonicalize_errors(
    errors: Optional[Union[Dict[str, Any], List[Any], str]]
) -> Dict[str, Any]:
    """
    Convert various error shapes to a JSON-serializable dict:
      - dict stays dict (values are stringified if needed)
      - list -> {"messages": [str(...), ...]}
      - str  -> {"messages": [str]}
      - None -> {}
    """
    if errors is None:
        return {}

    if isinstance(errors, dict):
        out: Dict[str, Any] = {}
        for k, v in errors.items():
            # Ensure values are JSON safe (stringify anything odd)
            if isinstance(v, (str, int, float, bool)) or v is None:
                out[str(k)] = v
            else:
                out[str(k)] = json.dumps(v, default=str)
        return out

    if isinstance(errors, list):
        return {"messages": [str(x) for x in errors]}

    # str or other scalars
    return {"messages": [str(errors)]}


def _merge_errors(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    """
    Shallow merge:
      - keys in incoming overwrite base
      - if both have "messages" (list), they are concatenated and de-duplicated (order preserved)
    """
    if not base:
        return dict(incoming)

    merged = dict(base)
    for k, v in incoming.items():
        if k == "messages":
            base_msgs = merged.get("messages", [])
            if not isinstance(base_msgs, list):
                base_msgs = [str(base_msgs)]
            new_msgs = v if isinstance(v, list) else [v]
            # preserve order, de-dup
            seen = set()
            combined: List[str] = []
            for m in [*base_msgs, *new_msgs]:
                sm = str(m)
                if sm not in seen:
                    seen.add(sm)
                    combined.append(sm)
            merged["messages"] = combined
        else:
            merged[k] = v
    return merged


def log_incomplete_data(
    session: Session,
    specimen_id: Optional[str],
    patient_id: Optional[str],
    facility_id: Optional[int],
    errors: Optional[Union[Dict[str, Any], List[Any], str]],
    payload: Optional[dict],
    *,
    merge: bool = True,
) -> None:
    """
    Upsert into openhie_vl.incomplete_data_log using (specimen_identifier, payload_hash).

    - `merge=True` merges new errors into existing row's errors (messages lists are appended de-duplicated).
      Set `merge=False` to replace errors entirely.
    - Idempotent: if nothing changes, it won’t write.
    """
    try:
        key_specimen = _sanitize_id(specimen_id)
        key_patient  = _sanitize_id(patient_id)
        payload_hash = _hash_payload(payload)
        incoming_err = _canonicalize_errors(errors)

        # Look up existing row
        existing = (
            session.query(IncompleteDataLog)
            .filter(
                IncompleteDataLog.specimen_identifier == key_specimen,
                IncompleteDataLog.payload_hash == payload_hash,
            )
            .one_or_none()
        )

        if existing:
            # Decide final errors
            final_errors = (
                _merge_errors(existing.errors or {}, incoming_err)
                if merge else incoming_err
            )

            # Detect no-op to avoid needless writes
            dirty = False
            if final_errors != (existing.errors or {}):
                existing.errors = final_errors
                dirty = True

            if key_patient and key_patient != (existing.patient_identifier or ""):
                existing.patient_identifier = key_patient
                dirty = True

            if facility_id and facility_id != existing.facility_id:
                existing.facility_id = facility_id
                dirty = True

            if dirty:
                session.add(existing)
                session.commit()
                logger.info(f"🔁 Updated incomplete_data_log id={existing.id} ({key_specimen})")
            else:
                logger.info(f"ℹ️  No changes for incomplete_data_log ({key_specimen}); skipped write.")
        else:
            row = IncompleteDataLog(
                specimen_identifier=key_specimen,
                patient_identifier=key_patient,
                facility_id=facility_id,
                errors=incoming_err,
                payload_hash=payload_hash,
            )
            session.add(row)
            session.commit()
            logger.info(f"🆕 Inserted incomplete_data_log ({key_specimen})")

    except SQLAlchemyError as db_err:
        session.rollback()
        logger.warning(f"Failed to log incomplete data (DB): {db_err}")
    except Exception as e:
        session.rollback()
        logger.warning(f"Failed to log incomplete data: {e}")

def _buildKafkaSecurityOptions():
    """
    Build Kafka client keyword arguments for security based on environment:
    - DEV: PLAINTEXT (no auth)
    - PROD: SASL_PLAINTEXT + SCRAM-SHA-256 (username/password)
    """
    print(f"....auth 3.0 ....")
    securityProtocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    print(f"....protocol chosen: {securityProtocol} ");

    kafkaSecurityOptions = {"security_protocol": securityProtocol}

    if securityProtocol.startswith("SASL"):
        kafkaSecurityOptions.update({
            "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            "sasl_plain_username": os.getenv("KAFKA_USERNAME", ""),
            "sasl_plain_password": os.getenv("KAFKA_PASSWORD", ""),
        })
        # If you later use TLS:
        # caFile = os.getenv("SSL_CAFILE")
        # if caFile: kafkaSecurityOptions["ssl_cafile"] = caFile

    return kafkaSecurityOptions

def buildTopicFromDhis2Uid(dhis2_uid: str) -> str:
    """Sanitize DHIS2 UID into a valid Kafka topic name."""
    if not dhis2_uid:
        raise ValueError("dhis2_uid is required")
    return re.sub(r'[^a-zA-Z0-9._-]', '_', dhis2_uid.strip()).lower()

def buildMessageKey(patient_identifier: str, specimen_identifier: str) -> bytes:
    """Composite key as bytes: 'patient|specimen'."""

    if not patient_identifier or not specimen_identifier:
        raise ValueError("Both patient_identifier and specimen_identifier are required")
        
    sanitized_patient_identifier = sanitize_art_number(patient_identifier)
    return f"{sanitized_patient_identifier}|{specimen_identifier}".encode("utf-8")

def _gt_zero(x) -> bool:
    try:
        return int(x) > 0
    except (TypeError, ValueError):
        return False