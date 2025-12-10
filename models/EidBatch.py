# models/EidBatch.py
from sqlalchemy import (
    Column, Integer, String, Date, Enum, TIMESTAMP, text
)
from sqlalchemy.orm import relationship
from datetime import datetime
from models.base import Base


class EidBatch(Base):
    __tablename__ = "batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lab = Column(String(16), default="CPHL")
    batch_number = Column(String(32), unique=True)
    facility_id = Column(Integer)
    facility_name = Column(String(255))
    facility_district = Column(String(255))
    senders_name = Column(String(75))
    senders_telephone = Column(String(50))
    date_dispatched_from_facility = Column(Date)
    date_rcvd_by_cphl = Column(Date)
    date_entered_in_DB = Column(Date, default=datetime.now)
    tests_requested = Column(
        Enum("PCR", "SCD", "BOTH_PCR_AND_SCD", "UNKNOWN"),
        default="PCR"
    )
    source_system = Column(Integer, default=1)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    # Relationship
    samples = relationship("EidDbsSample", back_populates="batch")
