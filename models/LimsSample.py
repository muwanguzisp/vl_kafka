from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class LimsSample(Base):
    __tablename__ = 'vl_samples'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    form_number = Column(String(255))
    pregnant = Column(String(1), nullable=True)  # Y/N/NULL
    breast_feeding = Column(String(1), nullable=True)  # Y/N/NULL
    consented_sample_keeping = Column(String(1), nullable=True)  # Y/N/NULL

    active_tb_status = Column(String(255), nullable=True)
    date_collected = Column(Date, nullable=True)
    treatment_initiation_date = Column(Date, nullable=True)
    sample_type = Column(String(255), nullable=True)

    verified = Column(Boolean, default=False)
    in_worksheet = Column(Boolean, default=False)

    arv_adherence_id = Column(String(255), nullable=True)
    current_regimen_id = Column(Integer, nullable=True)
    facility_id = Column(Integer)
    patient_id = Column(Integer)

    treatment_indication_id = Column(String(255), nullable=True)
    treatment_line_id = Column(String(255), nullable=True)
    viral_load_testing_id = Column(Integer, nullable=True)
    clinician_id = Column(Integer, nullable=True)
    lab_tech_id = Column(Integer, nullable=True)

    treatment_duration = Column(String(255), nullable=True)
    treatment_care_approach = Column(String(255), nullable=True)
    current_who_stage = Column(String(255), nullable=True)

    is_study_sample = Column(Boolean, default=False)
    is_data_entered = Column(Boolean, default=False)

    facility_reference = Column(String(255), nullable=True)
    stage = Column(String(255), nullable=True)
    required_verification = Column(Boolean, default=False)

    hie_data_created_at = Column(DateTime, nullable=True)
    source_system = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
