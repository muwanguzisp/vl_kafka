def generate_fhir_response(status: str, narrative: str, data: dict) -> dict:
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

    elif status == "fatal-error":
        return {
            "resourceType": "Bundle",
            "type": "message",
            "timestamp": data["time_stamp"],
            "entry": [
                {
                    "resource": {
                        "resourceType": "MessageHeader",
                        "response": {
                            "code": "fatal-error",
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
        }

    elif status == "fatal-error-test_request-exists":
        return {
            "resourceType": "Bundle",
            "type": "message",
            "timestamp": data["time_stamp"],
            "entry": [
                {
                    "resource": {
                        "resourceType": "MessageHeader",
                        "response": {
                            "code": "fatal-error",
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

    else:
        # Optional: handle unknown status
        return {
            "resourceType": "OperationOutcome",
            "issue": [
                {
                    "severity": "error",
                    "code": "invalid-status",
                    "details": {
                        "text": f"Unrecognized status '{status}'"
                    }
                }
            ]
        }
