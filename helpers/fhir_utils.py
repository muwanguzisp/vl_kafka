import mysql.connector
from datetime import datetime
import re
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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