# Project: Viral Load Kafka API Gateway

## 📦 Description
This project is a FastAPI-based gateway that:
- Accepts FHIR Viral Load (VL) bundles from EMRs via HTTP POST
- Validates the payload for completeness and integrity
- Sends valid data to a Kafka topic (`vl_test_request`)
- Listens to Kafka topics and writes into a MySQL LIMS database
- Can also lookup test results in the LIMS DB and publish to Kafka

---

## 🗂️ Folder Structure
```
vl_kafka_api/
├── main.py                  # FastAPI entrypoint
├── validator.py             # Payload validation logic
├── kafka_producer.py        # Publishes data to Kafka
├── kafka_consumer.py        # Consumes Kafka data (optional for logs)
├── lims_consumer.py         # Reads Kafka and writes to LIMS MySQL
├── lims_result_lookup.py    # Retrieves LIMS results based on UID/barcode
├── requirements.txt         # Python dependencies
```

---

## 🚀 Getting Started

### ✅ Prerequisites
- Python 3.8+
- MySQL access (for LIMS + transaction logs)
- Kafka broker (no authentication needed for now)

### 🛠️ Installation
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 🔁 Run the API
```bash
uvicorn main:app --host 0.0.0.0 --port 3000
```
API will be accessible at: `http://localhost:3000/docs`

---

## 🔒 Kafka Topics
- `vl_test_request` – Receives new VL requests from EMR
- `vl_test_results` – Sends test results back to EMR

---

## 🧪 API Endpoint
### `POST /api/post_vl_request`
- Accepts a full FHIR transaction bundle
- Validates key fields like ART number, DHIS2 UID, treatment indication
- Returns:
```json
{
  "message": "✅ Payload accepted and dispatched to Kafka."
}
```

On error:
```json
{
  "detail": {
    "missing_fields": ["patient.gender", "sample.form_number"]
  }
}
```

---

## 🗃️ LIMS MySQL Tables
- `vl_patients` (use `sanitized_art_number` + `facility_id` for uniqueness)
- `vl_samples` (match using `facility_reference`)
- `transaction_logs` (in `openhie_vl` DB, check for duplicates)

---

## 🛠️ Deployment (recommended)
- Use `gunicorn` with `uvicorn.workers.UvicornWorker`
- Use `nginx` as reverse proxy on port 80/443
- Systemd service example provided in deployment guide

---

## 👤 Author
Simon Peter Muwanguzi  
Makerere University METS Program / CPHL

---

## 📄 License
MIT License
