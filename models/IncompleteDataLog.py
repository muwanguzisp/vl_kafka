# models/IncompleteDataLog.py
from sqlalchemy import Column, Integer, String, JSON, TIMESTAMP, func, UniqueConstraint, Index
from openhie_vl import BaseOpenHIE
from sqlalchemy.orm import declarative_base




class IncompleteDataLog(BaseOpenHIE):
    __tablename__ = "incomplete_data_log"
    __table_args__ = (
        UniqueConstraint("specimen_identifier", "payload_hash", name="uq_incomplete_specimen"),
        Index("idx_specimen_identifier", "specimen_identifier"),
        Index("idx_facility_id_created_at", "facility_id", "created_at"),
         
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    specimen_identifier = Column(String(191), nullable=False)
    patient_identifier  = Column(String(191), nullable=True)
    facility_id         = Column(Integer, nullable=True)
    errors              = Column(JSON, nullable=False)      # requires MySQL 5.7+
    payload_hash        = Column(String(64), nullable=False)
    created_at          = Column(TIMESTAMP, server_default=func.now())
    updated_at          = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

