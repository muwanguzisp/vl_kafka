# create_tables.py
from openhie_vl import engine_openhie, BaseOpenHIE
from models.IncompleteDataLog import IncompleteDataLog

BaseOpenHIE.metadata.create_all(bind=engine_openhie)
print("✅ OpenHIE tables ensured.")
