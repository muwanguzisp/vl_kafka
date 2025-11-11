# openhie_vl.py
import os
from urllib.parse import quote_plus
from pathlib import Path 
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

ENV_PATH = Path.cwd() / ".env"
load_dotenv(dotenv_path=ENV_PATH)

user = quote_plus(os.getenv("OPENHIE_DB_USER", "homestead"))
pwd  = quote_plus(os.getenv("OPENHIE_DB_PASS", "secret"))
host = os.getenv("OPENHIE_DB_HOST", "192.168.1.144")
port = os.getenv("OPENHIE_DB_PORT", "3306")
name = os.getenv("OPENHIE_DB_NAME", "openhie_vl_20250312")



OPENHIE_DATABASE_URL = (
    f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{name}?charset=utf8mb4"
)

engine_openhie = create_engine(
    OPENHIE_DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=5,
    max_overflow=10,
    echo=False,
)

metadata_openhie = MetaData()
BaseOpenHIE = declarative_base(metadata=metadata_openhie)
SessionOpenHIE = sessionmaker(bind=engine_openhie, autocommit=False, autoflush=False)
