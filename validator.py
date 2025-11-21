
from fastapi import HTTPException
import re
from typing import Any, Optional 
from datetime import datetime
from typing import List, Dict, Union
from helpers.fhir_utils import (
    _gt_zero,
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
from helpers.fhir_response_utils import generate_fhir_response,generate_multiple_fhir_responses

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

def validate_vl_payload_mini(bundle,session):
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

            uid = None
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

                facility_id = get_lims_facility_id(session, uid)
                if not facility_id:
                    raise HTTPException(
                        status_code=422,
                        detail=generate_multiple_fhir_responses(
                            status="fatal-error",
                            narrative_list=[f"DHIS2 UID {uid} not in LIMS. Please ask for a correct one. "],
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

def validate_vl_legacy_bio_data(payload, session):
    """
    Validate legacy VL bio-data payload (ServiceRequest) before sending to Kafka.

    Mapping (from your legacy JSON):
      - DHIS2 UID      -> payload.locationCode
      - Specimen ID    -> payload.specimen[0].identifier
      - Patient ID     -> payload.specimen[0].subject.identifier  (ART / facility patient id)
      - Date collected -> payload.specimen[0].collection.collectedDateTime
      - Sample type    -> payload.specimen[0].type               (e.g. "Plasma")
      - Lab contact    -> payload.specimen[0].collection.collector
      - Clinician      -> payload.requester
    """

    # --- Basic shape & resourceType check ---
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail=generate_fhir_response(
                status="fatal-error",
                narrative="Unkown Service Request",
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    resource_type = payload.get("resourceType")
    if resource_type != "ServiceRequest":
        raise HTTPException(
            status_code=400,
            detail=generate_fhir_response(
                status="fatal-error",
                narrative="Unknown ServiceRequest resource type",
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
        "lab_contact": {},
        "clinician": {}
    }

    # --- DHIS2 UID (facility) from locationCode (location id) ---
    dhis2_uid = payload.get("locationCode")
    if dhis2_uid:
        extracted["dhis2_uid"] = dhis2_uid

        # 1) Basic format validation for DHIS2 UID
        if not re.fullmatch(r"[A-Za-z0-9]{9,15}", dhis2_uid):
            raise HTTPException(
                status_code=400,
                detail=generate_fhir_response(
                    status="fatal-error",
                    narrative="The locationCode or DHIS2 UID does not seem be valid. Check with your administrator",
                    data={
                        "time_stamp": datetime.now().isoformat(),
                        "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
                        "lims_sample_id": None,
                        "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                    }
                )
            )

        # 2) Check that facility exists in LIMS
        facility_id = get_lims_facility_id(session, dhis2_uid)
        if not facility_id:
            raise HTTPException(
                status_code=400,
                detail=generate_fhir_response(
                    status="fatal-error",
                    narrative=f"The locationCode or DHIS2 UID does not seem be valid. Check with your administrator",
                    data={
                        "time_stamp": datetime.now().isoformat(),
                        "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
                        "lims_sample_id": None,
                        "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                    }
                )
            )

    # --- Specimen (form_number, date_collected, sample_type, patient id, lab contact) ---
    specimens = payload.get("specimen") or []
    if not specimens:
        raise HTTPException(
            status_code=400,
            detail=generate_fhir_response(
                status="fatal-error",
                narrative="No specimen found in ServiceRequest",
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": None,
                    "patient_identifier": ""
                }
            )
        )

    specimen = specimens[0]

    # specimen identifier (form_number)
    form_number = specimen.get("identifier")
    extracted["sample"]["form_number"] = form_number
    extracted["sample"]["facility_reference"] = form_number

    if form_number:
        if not is_specimen_identifier_valid(form_number):
            raise HTTPException(
                status_code=400,
                detail=generate_fhir_response(
                    status="fatal-error",
                    narrative=f"The specimen ID: {form_number} is not HIE compliant. Please secure a valid barcode matching this Year",
                    data={
                        "time_stamp": datetime.now().isoformat(),
                        "specimen_identifier": form_number,
                        "lims_sample_id": None,
                        "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                    }
                )
            )
    else:
        logger.info("Legacy bio payload missing specimen.identifier (form_number)")

    # date collected
    collection = specimen.get("collection") or {}
    extracted["sample"]["date_collected"] = collection.get("collectedDateTime")

    # sample type (raw; you can map later to your codes)
    sample_type_raw = specimen.get("type")
    extracted["sample"]["sample_type"] = sample_type_raw

    # lab contact person (collector)
    collector = collection.get("collector") or {}
    extracted["lab_contact"]["name"] = collector.get("name")
    extracted["lab_contact"]["phone"] = collector.get("telecom")

    # patient identifier (ART / facility patient id)
    subject = specimen.get("subject") or {}
    art_number = subject.get("identifier")
    extracted["patient"]["art_number"] = art_number

    # --- Clinician = requester ---
    requester = payload.get("requester") or {}
    extracted["clinician"]["name"] = requester.get("name")
    extracted["clinician"]["phone"] = requester.get("telecom")

    # --- Check mandatory fields (minimum to not deny service) ---
    missing = []

    if not extracted["dhis2_uid"]:
        missing.append("dhis2_uid")
    if not extracted["patient"].get("art_number"):
        missing.append("art_number")
    if not extracted["sample"].get("form_number"):
        missing.append("form_number")
    if not extracted["sample"].get("date_collected"):
        missing.append("date_collected")
    if not extracted["sample"].get("sample_type"):
        missing.append("sample_type")

    # Note: lab_contact and clinician are important but NOT required to accept the sample

    if missing:
        for missing_instance in missing:
            logger.info(f"Legacy bio missing field: {missing_instance}")
        logger.info(f"Missing fields: {missing}")

        error_messages = [
            FIELD_ERROR_MESSAGES[field]
            for field in missing
            if field in FIELD_ERROR_MESSAGES
        ]

        fhir_response_data = {
            "time_stamp": datetime.now().isoformat(),
            "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
            "lims_sample_id": None,
            "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
        }

        if not error_messages:
            logger.warning("No error messages found for missing fields in legacy bio validator!")

        for msg in error_messages:
            logger.info(f"Legacy bio error message: {msg}")

        raise HTTPException(
            status_code=422,
            detail=generate_multiple_fhir_responses(
                "fatal-error",
                error_messages,
                fhir_response_data
            )
        )

    # --- If all good, return the extracted minimal data + lab contact & clinician ---
    return {
        "patient": extracted["patient"],
        "sample": extracted["sample"],
        "dhis2_uid": extracted["dhis2_uid"],
        "lab_contact": extracted["lab_contact"],
        "clinician": extracted["clinician"],
    }



def validate_vl_legacy_program_data(payload, session):
    """
    Validate legacy VL program-data payload (Observation) before sending to Kafka.

    Required (to avoid denying service):
      - dhis2_uid                    -> contained[Patient].managingOrganization.reference
      - art_number (ART)            -> subject.reference
      - form_number (specimen id)   -> specimen.identifier
      - dob (birthDate)             -> contained[Patient].birthDate
      - gender                      -> contained[Patient].gender
      - treatment_initiation_date   -> component[...413946009...].valueDateTime / valueDate

    The rest of the program info (DSDM, adherence, TB, etc.) is handled later in the consumer.
    """

    # --- Basic shape & resourceType check ---
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["Invalid Observation"],
                data={
                    "time_stamp": datetime.now().isoformat(),
                    "specimen_identifier": "",
                    "lims_sample_id": "",
                    "patient_identifier": ""
                }
            )
        )

    resource_type = payload.get("resourceType")
    if resource_type != "Observation":
        raise HTTPException(
            status_code=400,
            detail=generate_multiple_fhir_responses(
                status="fatal-error",
                narrative_list=["Unkown Observation"],
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
    }

    # --- Patient ID (ART number) from Observation.subject.reference ---
    # e.g. "subject": { "reference": "LIDC-30717-AB", "type": "Patient" }
    subject = payload.get("subject") or {}
    art_number = subject.get("reference")
    extracted["patient"]["art_number"] = art_number

    # --- Specimen ID from Observation.specimen.identifier ---
    specimen = payload.get("specimen") or {}
    form_number = specimen.get("identifier")
    extracted["sample"]["form_number"] = form_number
    extracted["sample"]["facility_reference"] = form_number

    if form_number:
        if not is_specimen_identifier_valid(form_number):
            raise HTTPException(
                status_code=400,
                detail=generate_multiple_fhir_responses(
                    status="fatal-error",
                    narrative_list=[f"The specimen ID: {form_number} is not HIE compliant. Please secure a valid barcode matching this Year"],
                    data={
                        "time_stamp": datetime.now().isoformat(),
                        "specimen_identifier": form_number,
                        "lims_sample_id": None,
                        "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                    }
                )
            )
    else:
        logger.info("Legacy program payload missing specimen.identifier (form_number)")

    # --- DHIS2 UID + demographic info from contained.Patient ---
    dhis2_uid = None
    patient_resource = None
    for res in payload.get("contained", []):
        if res.get("resourceType") == "Patient":
            patient_resource = res
            org = res.get("managingOrganization") or {}
            dhis2_uid = org.get("reference")
            break

    extracted["dhis2_uid"] = dhis2_uid

    if dhis2_uid:
        # 1) Basic DHIS2 UID format check
        if not re.fullmatch(r"[A-Za-z0-9]{9,15}", dhis2_uid):
            raise HTTPException(
                status_code=400,
                detail=generate_multiple_fhir_responses(
                    status="fatal-error",
                    narrative_list=[f"The locationCode or DHIS2 UID does not seem be valid. Check with your administrator"],
                    data={
                        "time_stamp": datetime.now().isoformat(),
                        "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
                        "lims_sample_id": None,
                        "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                    }
                )
            )

        # 2) Check facility exists in LIMS
        facility_id = get_lims_facility_id(session, dhis2_uid)
        if not facility_id:
            raise HTTPException(
                status_code=400,
                detail=generate_multiple_fhir_responses(
                    status="fatal-error",
                    narrative_list=[f"The locationCode or DHIS2 UID does not seem be valid. Check with your administrator"],
                    data={
                        "time_stamp": datetime.now().isoformat(),
                        "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
                        "lims_sample_id": None,
                        "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
                    }
                )
            )
    else:
        logger.info("Legacy program payload missing DHIS2 UID (managingOrganization.reference)")

    # --- Gender & DOB from contained.Patient ---
    if patient_resource:
        extracted["patient"]["gender"] = patient_resource.get("gender")
        extracted["patient"]["dob"] = patient_resource.get("birthDate")

    # --- Treatment initiation date from components (SNOMED 413946009 / 'Treatment initiation') ---
    treatment_initiation_date = None
    for comp in payload.get("component", []):
        code_block = comp.get("code") or {}
        codings = code_block.get("coding") or []
        text = (code_block.get("text") or "").lower()

        matched = False
        for coding in codings:
            c_code = (coding.get("code") or "").lower()
            c_display = (coding.get("display") or "").lower()
            if (
                c_code == "413946009"
                or "treatment initiation" in text
                or "date treatment started" in c_display
            ):
                matched = True
                break

        if matched:
            v = comp.get("valueDateTime") or comp.get("valueDate")
            treatment_initiation_date = v
            break

    extracted["patient"]["treatment_initiation_date"] = treatment_initiation_date

    # --- Check mandatory fields ---
    missing = []

    if not extracted["dhis2_uid"]:
        missing.append("dhis2_uid")
    if not extracted["patient"].get("art_number"):
        missing.append("art_number")
    if not extracted["sample"].get("form_number"):
        missing.append("form_number")
    if not extracted["patient"].get("dob"):
        missing.append("dob")
    if not extracted["patient"].get("gender"):
        missing.append("gender")
    if not extracted["patient"].get("treatment_initiation_date"):
        missing.append("treatment_initiation_date")

    if missing:
        for missing_instance in missing:
            logger.info(f"Legacy program missing field: {missing_instance}")
        logger.info(f"Missing fields (legacy program): {missing}")

        error_messages = [
            FIELD_ERROR_MESSAGES[field]
            for field in missing
            if field in FIELD_ERROR_MESSAGES
        ]

        fhir_response_data = {
            "time_stamp": datetime.now().isoformat(),
            "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
            "lims_sample_id": None,
            "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
        }

        if not error_messages:
            logger.warning("No error messages found for missing fields in legacy program validator!")

        for msg in error_messages:
            logger.info(f"Legacy program error message: {msg}")

        raise HTTPException(
            status_code=422,
            detail=generate_multiple_fhir_responses(
                "fatal-error",
                error_messages,
                fhir_response_data
            )
        )

    # --- If all good, return the extracted minimal data ---
    return {
        "patient": extracted["patient"],
        "sample": extracted["sample"],
        "dhis2_uid": extracted["dhis2_uid"]
    }


def _norm_str(s: Optional[str]) -> str:
    return (s or "").strip()

def _clean_system(system: Optional[str]) -> str:
    s = _norm_str(system).rstrip("/")  # trim + drop trailing slash
    low = s.lower()
    if low in ("http://loinc.org", "https://loinc.org", "loinc", "loinc.org"):
        return "http://loinc.org"
    if low.startswith("http://snomed.info/sct") or low in ("snomed", "snomed ct", "sct"):
        return "http://snomed.info/sct"
    return s

def _normalize_coding_list(coding):
    out = []
    for c in (coding or []):
        out.append({
            "system": _clean_system(c.get("system")),
            "code": _norm_str(c.get("code")),
            "display": (None if _norm_str(c.get("display")).lower() == "null" else _norm_str(c.get("display")))
        })
    return out

def _preflight_clean(bundle: dict) -> dict:
    """Light clean of codeableConcept codings in-place."""
    if not isinstance(bundle, dict):
        return bundle
    for e in bundle.get("entry", []):
        res = e.get("resource") or {}
        # code, valueCodeableConcept
        if "code" in res and isinstance(res["code"], dict):
            res["code"]["coding"] = _normalize_coding_list(res["code"].get("coding"))
        if "valueCodeableConcept" in res and isinstance(res["valueCodeableConcept"], dict):
            vcc = res["valueCodeableConcept"]
            vcc["coding"] = _normalize_coding_list(vcc.get("coding"))
        # Observation.category[*].coding
        if isinstance(res.get("category"), list):
            for cat in res["category"]:
                if isinstance(cat, dict) and "coding" in cat:
                    cat["coding"] = _normalize_coding_list(cat.get("coding"))
        # Encounter.type[*].coding
        if isinstance(res.get("type"), list):
            for t in res["type"]:
                if isinstance(t, dict) and "coding" in t:
                    t["coding"] = _normalize_coding_list(t.get("coding"))
        # Specimen.type.coding
        if res.get("resourceType") == "Specimen":
            typ = res.get("type")
            if isinstance(typ, dict) and "coding" in typ:
                typ["coding"] = _normalize_coding_list(typ.get("coding"))
    return bundle

def validate_vl_payload_thoroughly(bundle, session):
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

    # Pre-clean to normalize systems & "null" displays
    bundle = _preflight_clean(bundle)

    resource_type = (bundle.get("resourceType") or "").lower()
    bundle_type = (bundle.get("type") or "").lower()
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

    # -------------------
    # PASS 1: PATIENT → facility_id (needed by other steps)
    # -------------------
    facility_id = None
    art_number = None
    

    for entry in entries:
        res = entry.get("resource", {}) or {}
        if res.get("resourceType") == "Patient":
            identifiers = res.get("identifier", []) or []
            for ident in identifiers:
                system_value = (ident.get("system") or "").lower()
                if "health.go.ug/art_number" in system_value:
                    art_number = ident.get("value")
                    extracted["patient"]["art_number"] = art_number
                    extracted["patient"]["sanitized_art_number"] = sanitize_art_number(art_number)

            extracted["patient"]["dob"] = res.get("birthDate")
            extracted["patient"]["gender"] = get_gender_flag(res.get("gender"))

            managing_org = res.get("managingOrganization", {}) or {}
            identifier_obj = managing_org.get("identifier", {}) or {}
            system = identifier_obj.get("system", "")

            if isinstance(system, str) and "health.go.ug" in system.lower():
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
                facility_id = get_lims_facility_id(session, uid)
                extracted["facility_id"] = facility_id
                extracted["patient"]["unique_id"] = generate_unique_id(facility_id, art_number or "")
                extracted["patient"]["other_id"] = 'NULL'
                extracted["patient"]["facility_id"] = facility_id
                extracted["patient"]["created_by_id"] = 1



    # -------------------
    # PASS 2: Specimen, ServiceRequest, Observations (now facility_id is ready)
    # -------------------
    for entry in entries:
        res = entry.get("resource", {}) or {}
        rtype = res.get("resourceType")

        if rtype == "Specimen":
            form_number = res.get("id")
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

            collection = res.get("collection", {}) or {}
            extracted["sample"]["date_collected"] = collection.get("collectedDateTime")
            extracted["sample"]["sample_type"] = get_sample_type_from_bundle_element(entry)

            collector_reference = (collection.get("collector") or {}).get("reference")
            # Only resolve lab tech if we already have a valid facility_id
            if _gt_zero(facility_id):
                extracted["sample"]["lab_tech_id"] = get_lab_technician_id_from_entry_element(
                    reference=collector_reference,
                    payload=bundle,
                    selected_facility_id=facility_id,
                    session=session
                )

        elif rtype == "ServiceRequest":
            # Only try to resolve clinician if facility_id is known
            if _gt_zero(facility_id):
                requester_reference = res.get("requester", {}).get("reference")
                if requester_reference:
                    clinician_id = get_clinician_id_from_reference(
                        requester_reference,
                        payload=bundle,
                        facility_id=facility_id,
                        session=session
                    )
                    extracted["sample"]["clinician_id"] = clinician_id

        elif rtype == "Observation":
            code_block = res.get("code", {}) or {}
            codings = _normalize_coding_list(code_block.get("coding"))

            def _pick_value_code_by_system(resource: dict, system_contains: str) -> Optional[str]:
                vcc = (resource or {}).get("valueCodeableConcept", {}) or {}
                for cc in _normalize_coding_list(vcc.get("coding")):
                    sys = (cc.get("system") or "").lower()
                    if system_contains.lower() in sys:
                        return cc.get("code")
                return None

            # ART initiation date → patient.treatment_initiation_date
            if not extracted["patient"].get("treatment_initiation_date"):
                art_start = get_art_initiation_date(entry)  # YYYY-MM-DD or None
                if art_start:
                    extracted["patient"]["treatment_initiation_date"] = art_start

            # WHO clinical stage
            if not extracted["sample"].get("current_who_stage"):
                who_stage = get_who_clinical_stage_from_element(entry)
                if who_stage is not None:
                    extracted["sample"]["current_who_stage"] = who_stage

            # Treatment indication
            if not extracted["sample"].get("treatment_indication_id"):
                ti = get_treatment_indication_id_from_element(entry)
                if ti is not None:
                    extracted["sample"]["treatment_indication_id"] = ti

            # Treatment line
            if not extracted["sample"].get("treatment_line_id"):
                tl = get_treatment_line_id_from_element(entry)
                if tl is not None:
                    extracted["sample"]["treatment_line_id"] = tl

            # ARV adherence
            if not extracted["sample"].get("arv_adherence_id"):
                adh = get_adherence_id_from_element(entry)
                if adh is not None:
                    extracted["sample"]["arv_adherence_id"] = adh

            # Care approach (CPHL 202501003)
            if not extracted["sample"].get("treatment_care_approach"):
                for c in codings:
                    system_val = (c.get("system") or "").lower()
                    code_val = c.get("code")
                    if ("www.cphl.go.ug" in system_val or "cphl" in system_val) and code_val == "202501003":
                        val = _pick_value_code_by_system(res, "cphl")
                        if val:
                            extracted["sample"]["treatment_care_approach"] = val
                        break

            if not extracted["sample"].get("treatment_care_approach"):
                for c in codings:
                    system_val = (c.get("system") or "").lower()
                    if "cphl" in system_val and c.get("code") == "202501003":
                        care_code = _pick_value_code_by_system(res, "cphl")
                        mapped_id = get_treatment_care_approach(care_code)
                        if mapped_id is not None:
                            extracted["sample"]["treatment_care_approach"] = mapped_id
                        break

    # -------------------
    # MISSING FIELDS & ERROR PACK
    # -------------------
    missing = []
    if not extracted["dhis2_uid"]:
        missing.append("dhis2_uid")
    for f in ["art_number", "dob", "gender", "treatment_initiation_date"]:
        if not extracted["patient"].get(f):
            missing.append(f)
    for f in ["form_number", "facility_reference", "date_collected", "sample_type",
              "current_who_stage","treatment_indication_id","treatment_line_id",
              "arv_adherence_id","treatment_care_approach"]:
        if not extracted["sample"].get(f):
            missing.append(f)

    error_log_data = None
    if missing:
        for m in missing:
            logger.info(f"....{m}")
        logger.info(f"Missing fields: {missing}")

        # FIX: build list of strings for build_error_log_entry
        error_messages_list = [FIELD_ERROR_MESSAGES.get(field, f"{field} is required") for field in missing]

        error_log_data = build_error_log_entry(bundle, error_messages_list)
        fhir_response_data = {
            "time_stamp": datetime.now().isoformat(),
            "specimen_identifier": extracted["sample"].get("form_number", "unknown"),
            "lims_sample_id": None,
            "patient_identifier": extracted["patient"].get("art_number", "Patient/unknown")
        }

        if not error_messages_list:
            logger.warning("No error messages found for missing fields!")
        logger.info("..Error messages..")
        for msg in error_messages_list:
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
