from fastapi import FastAPI, Request, HTTPException
from validator import validate_vl_payload
from kafka_producer import send_to_kafka

app = FastAPI()

@app.post("/single_payload")
async def post_vl_single_payload_request(request: Request):
    payload = await request.json()
    try:
        #validated_data = validate_vl_payload(payload)
        send_to_kafka("vl_single_payload_request", payload)
        return {"message": "✅ Payload accepted and dispatched to Kafka."}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@app.post("/test_request")
async def post_vl_test_request(request: Request):
    payload = await request.json()
    try:
        send_to_kafka("vl_test_request_bio_data", payload)
        return {"message": "✅ Payload accepted and dispatched to Kafka."}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@app.post("/test_details")
async def post_vl_test_details(request: Request):
    payload = await request.json()
    try:
        
        send_to_kafka("vl_test_request_program_data", payload)
        return {"message": "✅ Payload accepted and dispatched to Kafka."}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@app.post("/sample_result")
async def query_result(request: Request):
    try:
        data = await request.json()
        dhis2_uid = data.get("dhis2_uid")
        barcode = data.get("barcode")
        if not dhis2_uid or not barcode:
            return JSONResponse(status_code=422, content={"error": "Missing dhis2_uid or barcode"})

        result = lookup_and_publish_result(dhis2_uid, barcode)
        if result:
            return {"status": "published", "result": result}
        else:
            return JSONResponse(status_code=404, content={"message": "No matching result found"})

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})