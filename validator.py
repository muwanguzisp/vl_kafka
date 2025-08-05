from fastapi import HTTPException
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    

def validate_vl_payload(bundle):
    if not isinstance(bundle, dict):
        raise HTTPException(status_code=400, detail="Invalid bundle structure")

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
            if managing_org.get("identifier", {}).get("system") == "https://hmis.health.go.ug/":
                extracted["dhis2_uid"] = managing_org["identifier"].get("value")

        elif resource_type == "Specimen":
            extracted["sample"]["form_number"] = resource.get("id")
            extracted["sample"]["facility_reference"] = resource.get("id")
            collection = resource.get("collection", {})
            extracted["sample"]["date_collected"] = collection.get("collectedDateTime")

        elif resource_type == "Observation":
            coding = resource.get("code", {}).get("coding", [])
            for c in coding:
                if c.get("system") == "www.cphl.go.ug" and c.get("code") == "202501009":
                    obs_coding = resource.get("valueCodeableConcept", {}).get("coding", [])
                    for obs in obs_coding:
                        if obs.get("system") == "www.cphl.go.ug":
                            extracted["observations"]["treatment_indication_id"] = obs.get("code")

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
    for f in ["form_number", "facility_reference", "date_collected"]:
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
