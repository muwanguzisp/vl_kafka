from sqlalchemy import Column, Integer, String, Date, ForeignKey,DateTime, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class LimsPatient(Base):
    __tablename__ = 'vl_patients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    unique_id = Column(String(255))
    art_number = Column(String(255))
    other_id = Column(String(255))
    gender = Column(String(10))
    dob = Column(Date)
    created_by_id = Column(Integer)
    facility_id = Column(Integer)
    treatment_initiation_date = Column(Date)
    sanitized_art_number = Column(String(255))

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
