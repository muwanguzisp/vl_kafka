# db.py
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()


# ⚡ Adjust with your MySQL credentials for the VL LIMS database
DB_USER = quote_plus(os.getenv("DB_USER", "homestead"))
DB_PASSWORD  = quote_plus(os.getenv("DB_PASSWORD", "secret"))
DB_HOST = os.getenv("DB_HOST", "192.168.1.144")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "vl_db")

# SQLAlchemy connection URL
DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
