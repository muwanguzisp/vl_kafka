# models/EidBatch.py

from sqlalchemy import (
    Column, Integer, String, Date, Enum, TIMESTAMP, text, SmallInteger
)
from sqlalchemy.orm import relationship
from datetime import datetime
from models.base import Base


class EidBatch(Base):
    __tablename__ = "batches"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # --- Core Identifiers ---
    lab = Column(String(16), default="CPHL", nullable=False)
    batch_number = Column(String(32), unique=True, nullable=True)

    # --- Entry & Facility Info ---
    entered_by = Column(Integer, nullable=True)
    facility_id = Column(Integer, nullable=True)
    facility_name = Column(String(255), nullable=True)
    facility_district = Column(String(255), nullable=True)
    requesting_unit = Column(String(250), nullable=True)
    is_single_form = Column(SmallInteger, default=1, nullable=False)

    # --- Sender Info ---
    senders_name = Column(String(75), nullable=True)
    senders_telephone = Column(String(50), nullable=True)
    senders_comments = Column(String(512), nullable=True)

    # --- Result Transport ---
    results_transport_method = Column(
        Enum("POSTA_UGANDA", "COLLECTED_FROM_LAB"),
        default="POSTA_UGANDA",
        nullable=False
    )
    results_return_address = Column(String(128), default="", nullable=False)

    # --- Dates ---
    date_dispatched_from_facility = Column(Date, nullable=True)
    date_rcvd_by_cphl = Column(Date, nullable=True)
    date_entered_in_DB = Column(Date, default=datetime.now().date, nullable=False)

    # --- Status Flags ---
    all_samples_rejected = Column(Enum("YES", "NO"), default="NO", nullable=False)
    PCR_results_released = Column(Enum("YES", "NO"), default="NO", nullable=False)
    SCD_results_released = Column(Enum("YES", "NO"), default="NO", nullable=False)
    qc_done = Column(Enum("YES", "NO"), default="NO", nullable=False)
    scd_qc_done = Column(Enum("YES", "NO"), default="NO", nullable=False)
    rejects_qc_done = Column(Enum("YES", "NO"), default="NO", nullable=False)
    f_paediatricART_available = Column(
        Enum("YES", "NO", "LEFT_BLANK"),
        default="NO",
        nullable=False
    )

    # --- Other Info ---
    tests_requested = Column(
        Enum("PCR", "SCD", "BOTH_PCR_AND_SCD", "UNKNOWN"),
        default="UNKNOWN",
        nullable=False
    )

    source_system = Column(Integer, default=4, nullable=False)

    # --- Audit Info ---
    updated_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )
    created_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # --- Relationships ---
    samples = relationship("EidDbsSample", back_populates="batch")
