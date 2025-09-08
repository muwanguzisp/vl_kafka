
from fastapi import HTTPException
import re
from datetime import datetime
from typing import List, Dict, Union
from helpers.fhir_utils import (
    generate_unique_id,
    get_sample_type_from_bundle_element,
    is_specimen_identifier_valid,
    get_adherence_id_from_element,
    get_clinician_id_from_reference,
    get_gender_flag,
    get_lims_facility_id,
    get_who_clinical_stage_from_element,
    get_art_initiation_date,
    get_treatment_line_id_from_element,
    get_treatment_indication_id_from_element,
    get_lab_technician_id_from_entry_element,
    get_treatment_care_approach,
    
    sanitize_art_number
)
from helpers.fhir_response_utils import generate_multiple_fhir_responses

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

FIELD_ERROR_MESSAGES = {
    "dhis2_uid": "The locationCode or DHIS2 UID does not seem be valid. Check with your administrator",
    "facility_id": "The locationCode or DHIS2 UID does not seem be valid. Check with your administrator",
    "art_number": "The patient identifier or ART number is required",
    "form_number": "The Form Number is required",
    "date_collected": "Specimen collection date is required",
    "dob": "Date of birth not known",
    "gender": "Gender not specified",
    "sample_type": "Specimen type not provided"  # ✅ Add this
}

def validate_vl_payload_mini(bundle):
    if not isinstance(bundle, dict):
        raise HTTPException(
            status_code=400,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["Invalid bundle structure"],
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    resource_type = bundle.get("resourceType", "").lower()
    bundle_type = bundle.get("type", "").lower()
    if resource_type != "bundle" or bundle_type != "transaction":
        raise HTTPException(
            status_code=400,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["Payload must be a Bundle of type 'transaction'"],
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    entries = bundle.get("entry", [])
    if not entries:
        raise HTTPException(
            status_code=422,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["No entries found in bundle"],
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    extracted = {
        "patient": {},
        "sample": {},
        "dhis2_uid": None,
        "observations": {},
        "servicerequest": {}
    }

    for entry in entries:
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if resource_type == "Patient":
            identifiers = resource.get("identifier", [])
            for ident in identifiers:
                system_value = ident.get("system", "").lower()
                if "health.go.ug/art_number" in system_value:
                    extracted["patient"]["art_number"] = ident.get("value")
            extracted["patient"]["dob"] = resource.get("birthDate")
            extracted["patient"]["gender"] = resource.get("gender")

            managing_org = resource.get("managingOrganization", {})
            identifier_obj = managing_org.get("identifier", {})
            system = identifier_obj.get("system", "")

            if isinstance(system, str) and "hmis.health.go.ug" in system.lower():
                extracted["dhis2_uid"] = identifier_obj.get("value")
                uid = extracted.get("dhis2_uid", "")

                if not re.fullmatch(r"[A-Za-z0-9]{9,15}", uid):
                    raise HTTPException(
                        status_code=422,
                        detail=generate_multiple_fhir_responses(
                            status="fatal-error",
                            narrative_list=[FIELD_ERROR_MESSAGES.get("dhis2_uid", f"Invalid DHIS2 UID: {uid}")],
                            data={
                                "time_stamp": datetime.now().isoformat(),
                                "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
                                "lims_sample_id": None,
                                "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                            }
                        )
                    )

        elif resource_type == "Specimen":
            form_number = resource.get("id")
            extracted["sample"]["form_number"] = form_number
            extracted["sample"]["facility_reference"] = form_number

            if not is_specimen_identifier_valid(form_number):
                raise HTTPException(
                    status_code=422,
                    detail=generate_multiple_fhir_responses(
                        status="fatal-error",
                        narrative_list=[f"The specimen identifier {form_number} is invalid"],
                        data={
                            "time_stamp": datetime.now().isoformat(),
                            "specimen_identifier": form_number,
                            "lims_sample_id": None,
                            "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                        }
                    )
                )

            collection = resource.get("collection", {})
            extracted["sample"]["date_collected"] = collection.get("collectedDateTime")
            extracted["sample"]["sample_type"] = get_sample_type_from_bundle_element(entry)
            extracted["sample"]["lab_tech"] = 2

        elif resource_type == "ServiceRequest":
            requester = resource.get("requester", {})
            if "reference" in requester:
                extracted["servicerequest"]["clinician_ref"] = requester["reference"]

    missing = []
    if not extracted["dhis2_uid"]:
        missing.append("dhis2_uid")
    for f in ["art_number", "dob", "gender"]:
        if not extracted["patient"].get(f):
            missing.append(f"{f}")
    for f in ["form_number", "facility_reference", "date_collected", "sample_type"]:
        if not extracted["sample"].get(f):
            missing.append(f"{f}")

    if missing:
        for missing_instance in missing:
            logger.info(f"....{missing_instance}")
        logger.info(f"Missing fields: {missing}")
        error_messages = [FIELD_ERROR_MESSAGES[field] for field in missing if field in FIELD_ERROR_MESSAGES]
        fhir_response_data = {
            "time_stamp": datetime.now().isoformat(),
            "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
            "lims_sample_id": None,
            "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
        }

        if not error_messages:
            logger.warning("No error messages found for missing fields!")
        logger.info(f"..Error messages..")
        for msg in error_messages:
            logger.info(f"....{msg}")
        raise HTTPException(status_code=422, detail=generate_multiple_fhir_responses("fatal-error", error_messages, fhir_response_data))

    return {
        "patient": extracted["patient"],
        "sample": extracted["sample"],
        "dhis2_uid": extracted["dhis2_uid"]
    }

def validate_vl_payload_thoroughly(bundle,session):
    if not isinstance(bundle, dict):
        raise HTTPException(
            status_code=400,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["Invalid bundle structure"],
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    resource_type = bundle.get("resourceType", "").lower()
    bundle_type = bundle.get("type", "").lower()
    if resource_type != "bundle" or bundle_type != "transaction":
        raise HTTPException(
            status_code=400,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["Payload must be a Bundle of type 'transaction'"],
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    entries = bundle.get("entry", [])
    if not entries:
        raise HTTPException(
            status_code=422,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["No entries found in bundle"],
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    extracted = {
        "patient": {},
        "sample": {},
        "dhis2_uid": None,
        "facility_id": None
    }
    
    error_log_data = None  # None unless errors found
    facility_id = 0
    for entry in entries:
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if resource_type == "Patient":
            identifiers = resource.get("identifier", [])
            for ident in identifiers:
                system_value = ident.get("system", "").lower()
                if "health.go.ug/art_number" in system_value:
                    extracted["patient"]["art_number"] = ident.get("value")
                    art_number = ident.get("value")
                    extracted["patient"]['sanitized_art_number'] = sanitize_art_number(art_number)

            extracted["patient"]["dob"] = resource.get("birthDate")
            extracted["patient"]["gender"] = get_gender_flag(resource.get("gender"))

            managing_org = resource.get("managingOrganization", {})
            identifier_obj = managing_org.get("identifier", {})
            system = identifier_obj.get("system", "")

            if isinstance(system, str) and "hmis.health.go.ug" in system.lower():
                extracted["dhis2_uid"] = identifier_obj.get("value")
                uid = extracted.get("dhis2_uid", "")

                if not re.fullmatch(r"[A-Za-z0-9]{9,15}", uid):
                    raise HTTPException(
                        status_code=422,
                        detail=generate_multiple_fhir_responses(
                            status="fatal-error",
                            narrative_list=[FIELD_ERROR_MESSAGES.get("dhis2_uid", f"Invalid DHIS2 UID: {uid}")],
                            data={
                                "time_stamp": datetime.now().isoformat(),
                                "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
                                "lims_sample_id": None,
                                "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                            }
                        )
                    )
                facility_id = get_lims_facility_id(session,uid)
                extracted["facility_id"] = facility_id
                extracted["patient"]['unique_id'] = generate_unique_id(facility_id,art_number)
                extracted["patient"]['other_id'] = 'NULL'
                extracted["patient"]["facility_id"] = facility_id
                extracted["patient"]["created_by_id"] = 1


        elif resource_type == "Specimen":
            form_number = resource.get("id")
            extracted["sample"]["form_number"] = form_number
            extracted["sample"]["facility_reference"] = form_number
            extracted["sample"]["facility_id"] = facility_id

            if not is_specimen_identifier_valid(form_number):
                raise HTTPException(
                    status_code=422,
                    detail=generate_multiple_fhir_responses(
                        status="fatal-error",
                        narrative_list=[f"The specimen identifier {form_number} is invalid"],
                        data={
                            "time_stamp": datetime.now().isoformat(),
                            "specimen_identifier": form_number,
                            "lims_sample_id": None,
                            "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                        }
                    )
                )

            collection = resource.get("collection", {})
            extracted["sample"]["date_collected"] = collection.get("collectedDateTime")
            extracted["sample"]["sample_type"] = get_sample_type_from_bundle_element(entry)

            collector_reference = (collection.get("collector") or {}).get("reference")
            extracted["sample"]["lab_tech_id"] = get_lab_technician_id_from_entry_element(
                reference=collector_reference,
                payload=bundle,               # the whole FHIR bundle
                selected_facility_id=facility_id,
                session=session)

        elif resource_type == "ServiceRequest":
            requester_reference = resource.get("requester", {}).get("reference")
            if requester_reference:
                clinician_id = get_clinician_id_from_reference(
                    requester_reference,
                    payload=bundle,
                    facility_id=facility_id,
                    session=session
                    )
                extracted["sample"]["clinician_id"] = clinician_id


        elif resource_type == "Observation":
            resource = entry.get("resource", {}) or {}
            code_block = resource.get("code", {}) or {}
            codings = code_block.get("coding", []) or []

            # --- tiny helper: pick valueCodeableConcept.coding[*].code by system ---
            def _pick_value_code_by_system(res: dict, system_contains: str) -> str | None:
                vcc = (res or {}).get("valueCodeableConcept", {}) or {}
                for cc in vcc.get("coding", []) or []:
                    sys = (cc.get("system") or "").lower()
                    if system_contains.lower() in sys:
                        return cc.get("code")
                return None

            # --- ART initiation date (SNOMED 413946009 or CIEL 9860155) -> patient.treatment_initiation_date ---
            if not extracted["patient"].get("treatment_initiation_date"):
                art_start = get_art_initiation_date(entry)  # returns YYYY-MM-DD or None
                if art_start:
                    extracted["patient"]["treatment_initiation_date"] = art_start

            # --- WHO clinical stage (SNOMED 385354005 -> value SNOMED mapped to stage 1..4) ---
            if not extracted["sample"].get("current_who_stage"):
                who_stage = get_who_clinical_stage_from_element(entry)
                if who_stage is not None:
                    extracted["sample"]["current_who_stage"] = who_stage

            # --- Treatment indication (CPHL 202501009) -> LIMS ID mapping ---
            if not extracted["sample"].get("treatment_indication_id"):
                ti = get_treatment_indication_id_from_element(entry)  # maps to {93,94,95,97,98,99,100,101} or None
                if ti is not None:
                    extracted["sample"]["treatment_indication_id"] = ti

            # --- Treatment line (CPHL 202501016) -> LIMS ID mapping ---
            if not extracted["sample"].get("treatment_line_id"):
                tl = get_treatment_line_id_from_element(entry)  # maps to {89,90,215} or None
                if tl is not None:
                    extracted["sample"]["treatment_line_id"] = tl

            # --- ARV adherence (LOINC LL5723-3) -> {1,2,3} ---
            if not extracted["sample"].get("arv_adherence_id"):
                adh = get_adherence_id_from_element(entry)
                if adh is not None:
                    extracted["sample"]["arv_adherence_id"] = adh

            # --- Care approach (CPHL 202501003) -> keep the CPHL code as-is ---
            if not extracted["sample"].get("treatment_care_approach"):
                for c in codings:
                    system_val = (c.get("system") or "").lower()
                    code_val = c.get("code")
                    if ("www.cphl.go.ug" in system_val or "cphl" in system_val) and code_val == "202501003":
                        val = _pick_value_code_by_system(resource, "cphl")
                        if val:
                            extracted["sample"]["treatment_care_approach"] = val
                        break

            # --- Active TB status (CPHL 202501002 -> value SNOMED code) ---
            if not extracted["sample"].get("active_tb_status"):
                for c in codings:
                    system_val = (c.get("system") or "").lower()
                    code_val = c.get("code")
                    if ("www.cphl.go.ug" in system_val or "cphl" in system_val) and code_val == "202501002":
                        val = _pick_value_code_by_system(resource, "snomed")
                        if val:
                            extracted["sample"]["active_tb_status"] = val
                        break

            if not extracted["sample"].get("treatment_care_approach"):
                for c in codings:
                    system_val = (c.get("system") or "").lower()
                    if "cphl" in system_val and c.get("code") == "202501003":
                        care_code = _pick_value_code_by_system(resource, "cphl")
                        mapped_id = get_treatment_care_approach(care_code)
                        if mapped_id is not None:
                            extracted["sample"]["treatment_care_approach"] = mapped_id
                        break




    missing = []
    if not extracted["dhis2_uid"]:
        missing.append("dhis2_uid")
    for f in ["art_number", "dob", "gender", "treatment_initiation_date"]:
        if not extracted["patient"].get(f):
            missing.append(f"{f}")
    for f in ["form_number", "facility_reference", "date_collected", "sample_type","current_who_stage","treatment_indication_id","treatment_line_id","arv_adherence_id","treatment_care_approach"]:
        if not extracted["sample"].get(f):
            missing.append(f"{f}")

    if missing:
        for missing_instance in missing:
            logger.info(f"....{missing_instance}")
        logger.info(f"Missing fields: {missing}")
        
        error_messages = {field: FIELD_ERROR_MESSAGES.get(field, f"{field} is required") for field in missing}

        error_log_data = build_error_log_entry(bundle, error_messages)
        fhir_response_data = {
            "time_stamp": datetime.now().isoformat(),
            "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
            "lims_sample_id": None,
            "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
        }

        if not error_messages:
            logger.warning("No error messages found for missing fields!")
        logger.info(f"..Error messages..")
        for msg in error_messages:
            logger.info(f"....{msg}")

    return {
        "patient": extracted["patient"],
        "sample": extracted["sample"],
        "dhis2_uid": extracted["dhis2_uid"],
        "facility_id": extracted["facility_id"],
        "errors": error_log_data
    }

def detect_source_system(bundle):
    known_sources = {
        "clinicmaster": 224,
        "ugandaemr": 223
    }

    # 1. Check Bundle.identifier.system
    identifier = bundle.get("identifier", {})
    identifier_system = identifier.get("system", "")
    for key in known_sources:
        if key.lower() in identifier_system.lower():
            return known_sources[key]

    entries = bundle.get("entry", [])
    
    for entry in entries:
        resource = entry.get("resource", {})

        # 2. Check MessageHeader.source fields
        if resource.get("resourceType") == "MessageHeader":
            source_info = resource.get("source", {})
            for val in source_info.values():
                if isinstance(val, str):
                    for key in known_sources:
                        if key.lower() in val.lower():
                            return known_sources[key]

        # 3. Check meta.source
        meta = resource.get("meta", {})
        if isinstance(meta.get("source", ""), str):
            for key in known_sources:
                if key.lower() in meta["source"].lower():
                    return known_sources[key]

        # 4. Check identifier.system
        for identifier in resource.get("identifier", []):
            system = identifier.get("system", "")
            for key in known_sources:
                if key.lower() in system.lower():
                    return known_sources[key]

        #5. Check Observation.code.coding[] for UgandaEMR system
        if resource.get("resourceType") == "Observation":
            code = resource.get("code",{})
            coding = code.get("coding",[])
            for key in coding:
                coding_system = key.get("system", "")
                for known_source_instance_key in known_sources:
                    if known_source_instance_key.lower() in coding_system:
                        return known_sources[known_source_instance_key]


    return "Unknown"






def build_error_log_entry(bundle: dict, error_messages: List[str]) -> Dict[str, Union[str, List[str]]]:
    """
    Constructs an error log dictionary to be saved into the error log table.

    Args:
        bundle (dict): The FHIR bundle with potential validation issues.
        error_messages (List[str]): List of human-readable error messages.

    Returns:
        dict: Structured dictionary suitable for error logging.
    """
    timestamp = datetime.now().isoformat()

    # Attempt to extract identifiers from bundle
    specimen_identifier = None
    art_number = None

    entries = bundle.get("entry", [])
    for entry in entries:
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if resource_type == "Specimen" and not specimen_identifier:
            specimen_identifier = resource.get("id")

        if resource_type == "Patient" and not art_number:
            for ident in resource.get("identifier", []):
                system_value = ident.get("system", "").lower()
                if "health.go.ug/art_number" in system_value:
                    art_number = ident.get("value")

    return {
        "timestamp": timestamp,
        "art_number": art_number or "unknown",
        "specimen_identifier": specimen_identifier or "unknown",
        "errors": error_messages
    }
