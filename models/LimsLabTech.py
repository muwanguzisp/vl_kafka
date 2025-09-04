from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class LimsLabTech(Base):
    __tablename__ = "vl_lab_techs"
    __table_args__ = {'mysql_charset': 'utf8mb4'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    lname = Column(String(255), nullable=True)
    lphone = Column(String(255), nullable=True)
    facility_id = Column(Integer)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
