# models/EidDbsSample.py

from sqlalchemy import (
    Column, Integer, String, Date, Enum, ForeignKey, SmallInteger,
    TIMESTAMP, text
)
from sqlalchemy.orm import relationship
from datetime import datetime
from models.base import Base


class EidDbsSample(Base):
    __tablename__ = "dbs_samples"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, ForeignKey("batches.id"), nullable=False)
    pos_in_batch = Column(SmallInteger, nullable=True, default=1)

    # --- Sample Verification & Status ---
    sample_rejected = Column(
        Enum("YES", "NO", "NOT_YET_CHECKED", "REJECTED_FOR_EID"),
        default="NOT_YET_CHECKED",
        nullable=False
    )

    # --- Infant Information ---
    infant_name = Column(String(255), nullable=True)
    infant_exp_id = Column(String(30), nullable=True)
    infant_gender = Column(
        Enum("MALE", "FEMALE", "NOT_RECORDED"),
        default="NOT_RECORDED",
        nullable=False
    )
    infant_age = Column(String(30), nullable=True)
    infant_dob = Column(Date, nullable=True)
    infant_is_breast_feeding = Column(
        Enum("YES", "NO", "UNKNOWN"),
        default="UNKNOWN",
        nullable=False
    )
    infant_entryPoint = Column(SmallInteger, nullable=True)
    infant_contact_phone = Column(String(20), nullable=True)

    # --- Preventive Care & PMTCT Info ---
    given_contri = Column(Enum("Y", "N", "BLANK"), default="BLANK", nullable=False)
    delivered_at_hc = Column(Enum("Y", "N", "BLANK"), default="BLANK", nullable=False)
    infant_feeding = Column(String(32), nullable=True)
    mother_htsnr = Column(String(32), nullable=True)
    mother_artnr = Column(String(32), nullable=True)

    # --- Form & Test Info ---
    is_single_form = Column(SmallInteger, default=0, nullable=False)
    test_type = Column(String(2), default="P", nullable=False)  # P = PCR
    infant_age_units = Column(String(10), default="months", nullable=True)
    mother_antenatal_prophylaxis = Column(SmallInteger, default=80, nullable=True)
    mother_delivery_prophylaxis = Column(SmallInteger, default=80, nullable=True)
    mother_postnatal_prophylaxis = Column(SmallInteger, default=80, nullable=True)
    infant_prophylaxis = Column(SmallInteger, nullable=True)

    # --- Dates ---
    date_dbs_taken = Column(Date, nullable=True)
    date_data_entered = Column(Date, default=datetime.now().date, nullable=False)

    # --- Test Request Info ---
    PCR_test_requested = Column(Enum("NO", "YES"), default="YES", nullable=False)
    SCD_test_requested = Column(Enum("NO", "YES"), default="NO", nullable=False)
    pcr = Column(
        Enum("FIRST", "SECOND", "NON_ROUTINE", "UNKNOWN", "THIRD"),
        default="UNKNOWN",
        nullable=False
    )

    # --- Legacy Flags ---
    migrated_to_old_schema = Column(Enum("YES", "NO"), default="NO", nullable=False)
    in_workSheet = Column(Enum("YES", "NO"), default="NO", nullable=False)
    pos_in_workSheet = Column(SmallInteger, default=0, nullable=False)
    ready_for_SCD_test = Column(
        Enum("YES", "NO", "TEST_NOT_NEEDED", "TEST_ALREADY_DONE"),
        default="NO",
        nullable=False
    )
    testing_completed = Column(Enum("YES", "NO"), default="NO", nullable=False)
    repeated_SC_test = Column(Enum("YES", "NO"), default="NO", nullable=False)

    # --- Administrative ---
    sender_id = Column(Integer, nullable=True)
    is_active = Column(Integer, default=0, nullable=False)

    # --- Audit Columns ---
    created_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # --- Relationship ---
    batch = relationship("EidBatch", back_populates="samples")
