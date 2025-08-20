
from fastapi import HTTPException
import re
from datetime import datetime
from helpers.fhir_utils import get_sample_type_from_bundle_element, is_specimen_identifier_valid
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

 

def validate_vl_payload(bundle):
    if not isinstance(bundle, dict):
        raise HTTPException(status_code=400, detail="Invalid bundle structure")

    resource_type = bundle.get("resourceType", "").lower()
    bundle_type = bundle.get("type", "").lower()
    if resource_type != "bundle" or bundle_type != "transaction":
        raise HTTPException(status_code=400, detail="Payload must be a Bundle of type 'transaction'")

    entries = bundle.get("entry", [])
    if not entries:
        raise HTTPException(status_code=422, detail="No entries found in bundle")

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

        logger.info(f"resource_type: .... {resource_type}")

        if resource_type == "Patient":
            identifiers = resource.get("identifier", [])
            for ident in identifiers:
                system_value = ident.get("system", "").lower()
                if "health.go.ug/art_number" in system_value :
                    extracted["patient"]["art_number"] = ident.get("value")
                    logger.info(f"art_number extracted: {extracted['patient']['art_number']}")
            extracted["patient"]["dob"] = resource.get("birthDate")
            extracted["patient"]["gender"] = resource.get("gender")

            
            managing_org = resource.get("managingOrganization", {})
            identifier_obj = managing_org.get("identifier", {})
            system = identifier_obj.get("system", "")
            
            if isinstance(system, str) and "hmis.health.go.ug" in system.lower():
                extracted["dhis2_uid"] = identifier_obj.get("value")
                uid = extracted.get("dhis2_uid", "")
                
                if not re.fullmatch(r"[A-Za-z0-9]{9,15}", uid):
                    raise HTTPException(status_code=422, detail={"invalid_dhis2_uid": uid})

            logger.info(f"log....0cc0....")


        elif resource_type == "Specimen":
            logger.info(f"log....00....")
            form_number = resource.get("id")
            extracted["sample"]["form_number"] = resource.get("id")
            extracted["sample"]["facility_reference"] = resource.get("id")

            if not is_specimen_identifier_valid(form_number):
                raise HTTPException(status_code=422, detail={"invalid_specimen_identifier": form_number})

            collection = resource.get("collection", {})
            
            logger.info(f"log....01....")

            extracted["sample"]["date_collected"] = collection.get("collectedDateTime")
            logger.info(f"log....02....")
            extracted["sample"]["sample_type"] = get_sample_type_from_bundle_element(entry)
            
            collector_reference = collection.get("collector", {}).get("reference")
            #extracted["sample"]["lab_tech_id"] = get_lab_tech_id_from_reference(collector_reference, bundle)
            extracted["sample"]["lab_tech"] = 2

        elif resource_type == "Observation":
            coding = resource.get("code", {}).get("coding", [])
            logger.info(f"log....i-01....")
            for coding_instance in coding:
                
                if "www.cphl.go.ug" in coding_instance.get("system", "") and coding_instance.get("code") == "202501009":
                    obs_coding = resource.get("valueCodeableConcept", {}).get("coding", [])
                    for obs in obs_coding:
                        if "www.cphl.go.ug" in obs.get("system", "") :
                            extracted["observations"]["treatment_indication_id"] = obs.get("code")
                logger.info(f"log....i-02....")

                if "www.cphl.go.ug" in coding_instance.get("system", "") and coding_instance.get("code") == "202501016":
                    obs_coding = resource.get("valueCodeableConcept", {}).get("coding", [])
                    for obs in obs_coding:
                        if "www.cphl.go.ug" in obs.get("system", "") :
                            extracted["observations"]["treatment_line_id"] = obs.get("code")

                if "www.cphl.go.ug" in coding_instance.get("system", "") and coding_instance.get("code") == "202501003":
                    obs_coding = resource.get("valueCodeableConcept", {}).get("coding", [])
                    for obs in obs_coding:
                        if "www.cphl.go.ug" in obs.get("system", "") :
                            extracted["observations"]["treatment_care_approach"] = obs.get("code")

                if "loinc" in coding_instance.get("system", "") and coding_instance.get("code") == "LL5723-3":
                    obs_coding = resource.get("valueCodeableConcept", {}).get("coding", [])
                    for obs in obs_coding:
                        if "loinc" in obs.get("system", "") :
                            extracted["observations"]["arv_adherence_id"] = obs.get("code")

                if "www.cphl.go.ug" in coding_instance.get("system", "") and coding_instance.get("code") == "202501002":
                    obs_coding = resource.get("valueCodeableConcept", {}).get("coding", [])
                    for obs in obs_coding:
                        if "snomed" in obs.get("system", "") :
                            extracted["observations"]["active_tb_status"] = obs.get("code")

                if "snomed" in coding_instance.get("system", "") and coding_instance.get("code") == "385354005":
                    obs_coding = resource.get("valueCodeableConcept", {}).get("coding", [])
                    for obs in obs_coding:
                        if "snomed" in obs.get("system", "") :
                            extracted["observations"]["current_who_stage"] = obs.get("code")

                if "snomed" in coding_instance.get("system", "") and coding_instance.get("code") == "413946009":
                    extracted["observations"]["current_who_stage"] = resource.get("valueDateTime")

        elif resource_type == "ServiceRequest":
            requester = resource.get("requester", {})
            if "reference" in requester:
                extracted["servicerequest"]["clinician_ref"] = requester["reference"]
            

    # Validate presence of required fields
    missing = []
    if not extracted["dhis2_uid"]:
        missing.append("dhis2_uid")
    for f in ["art_number", "dob", "gender"]:
        if not extracted["patient"].get(f):
            missing.append(f"patient.{f}")
    for f in ["form_number", "facility_reference", "date_collected","sample_type","lab_tech"]:
        if not extracted["sample"].get(f):
            missing.append(f"sample.{f}")
    if not extracted["observations"].get("treatment_indication_id"):
        missing.append("observation.treatment_indication_id")

    if missing:
        raise HTTPException(status_code=422, detail={"missing_fields": missing})

    return {
        "patient": extracted["patient"],
        "sample": extracted["sample"],
        "dhis2_uid": extracted["dhis2_uid"],
        "observations": extracted["observations"],
        "servicerequest": extracted["servicerequest"]
    }




