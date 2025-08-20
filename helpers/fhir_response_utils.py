from datetime import datetime
import uuid
import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def generate_fhir_response(status, narrative, data):
    """
    Generate a FHIR response bundle with a single error or success message.

    Args:
        status (str): e.g., "ok", "fatal-error"
        narrative (str): message for the client
        data (dict): {
            "time_stamp": ISO date string,
            "specimen_identifier": str,
            "lims_sample_id": str,
            "patient_identifier": str
        }

    Returns:
        dict: FHIR-compliant response bundle
    """
    if status == "ok":
        return {
            "resourceType": "Bundle",
            "type": "message",
            "timestamp": data["time_stamp"],
            "entry": [
                {
                    "resource": {
                        "resourceType": "MessageHeader",
                        "response": {
                            "code": "ok",
                            "details": {
                                "reference": f"Specimen/{data['specimen_identifier']}",
                                "text": narrative
                            }
                        }
                    }
                },
                {
                    "resource": {
                        "resourceType": "Specimen",
                        "subject": {
                            "reference": data["patient_identifier"],
                            "type": "Patient"
                        },
                        "identifier": data["specimen_identifier"],
                        "accessionIndentifier": data["lims_sample_id"]
                    }
                }
            ]
        }

    elif status in ["fatal-error", "fatal-error-test_request-exists"]:
        uuid_str = str(uuid.uuid4())
        entry = [
            {
                "resource": {
                    "resourceType": "MessageHeader",
                    "response": {
                        "code": "fatal-error",
                        "details": {
                            "reference": f"OperationOutcome/{uuid_str}"
                        }
                    }
                }
            },
            {
                "fullUrl": f"urn:uuid:{uuid_str}",
                "resource": {
                    "resourceType": "OperationOutcome",
                    "text": {
                        "status": "fatal-error"
                    },
                    "issue": [
                        {
                            "severity": "error",
                            "code": "fatal-error",
                            "details": {
                                "text": narrative
                            }
                        }
                    ]
                }
            }
        ]

        # Optionally append specimen info
        if status == "fatal-error-test_request-exists":
            entry.append({
                "resource": {
                    "resourceType": "Specimen",
                    "subject": {
                        "reference": data["patient_identifier"],
                        "type": "Patient"
                    },
                    "identifier": data["specimen_identifier"],
                    "accessionIndentifier": data["lims_sample_id"]
                }
            })

        return {
            "resourceType": "Bundle",
            "type": "message",
            "timestamp": data["time_stamp"],
            "entry": entry
        }

def generate_multiple_fhir_responses(status, narrative_list, data):
    issue_array = []

    for narrative in narrative_list:
        if narrative:
            logger.warning(f"......{narrative}....")
            issue_array.append({
                "severity": "error",
                "code": status,
                "details": {
                    "text": narrative
                }
            })

    return {
        "resourceType": "Bundle",
        "type": "message",
        "timestamp": data["time_stamp"],
        "entry": [
            {
                "resource": {
                    "resourceType": "MessageHeader",
                    "response": {
                        "code": status,
                        "details": {
                            "reference": "OperationOutcome/03f9aa7d-b395-47b9-84e0-053678b6e4e3"
                        }
                    }
                }
            },
            {
                "fullUrl": "urn:uuid:03f9aa7d-b395-47b9-84e0-053678b6e4e3",
                "resource": {
                    "resourceType": "OperationOutcome",
                    "text": {
                        "status": status
                    },
                    "issue": issue_array
                }
            },
            {
                "resource": {
                    "resourceType": "Specimen",
                    "subject": {
                        "reference": data["patient_identifier"],
                        "type": "Patient"
                    },
                    "identifier": data["specimen_identifier"],
                    "accessionIdentifier": data["lims_sample_id"]
                }
            }
        ]
    }
