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
    pos_in_batch = Column(SmallInteger, nullable=True)

    # Infant info
    infant_name = Column(String(255))
    infant_exp_id = Column(String(30))
    infant_gender = Column(
        Enum("MALE", "FEMALE", "NOT_RECORDED"), default="NOT_RECORDED"
    )
    infant_dob = Column(Date)
    infant_feeding = Column(String(32))
    given_contri = Column(Enum("Y", "N", "BLANK"), default="BLANK")

    # Test info
    test_type = Column(String(2))
    PCR_test_requested = Column(Enum("NO", "YES"), default="YES")
    SCD_test_requested = Column(Enum("NO", "YES"), default="NO")
    sample_rejected = Column(
        Enum("YES", "NO", "NOT_YET_CHECKED", "REJECTED_FOR_EID"),
        default="NOT_YET_CHECKED"
    )
    date_dbs_taken = Column(Date)
    date_data_entered = Column(Date, default=datetime.now)
    pcr = Column(
        Enum("FIRST", "SECOND", "NON_ROUTINE", "UNKNOWN", "THIRD"),
        default="UNKNOWN"
    )

    # Relationships
    batch = relationship("EidBatch", back_populates="samples")

    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
