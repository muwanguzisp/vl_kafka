import mysql.connector
from kafka_producer import send_to_kafka

def lookup_and_publish_result(dhis2_uid, barcode):
    db = mysql.connector.connect(
        host='localhost',
        user='your_lims_user',
        password='your_password',
        database='lims_db'
    )
    cursor = db.cursor(dictionary=True)

    cursor.execute("""
        SELECT * FROM vl_results
        WHERE dhis2_uid = %s AND barcode = %s
    """, (dhis2_uid, barcode))

    row = cursor.fetchone()
    db.close()

    if row:
        send_to_kafka("vl_hie_test_results", row)
        return row
    return None
