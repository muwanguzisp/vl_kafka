# db.py
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Be explicit about the .env path used by systemd WorkingDirectory
load_dotenv(dotenv_path="/opt/vl_kafka/.env")

# Read raw envs
raw_user = os.getenv("DB_USER", "vl_kafka")
raw_pwd  = os.getenv("DB_PASSWORD", "S0m3Str0ngP@ss!")
raw_host = os.getenv("DB_HOST", "127.0.0.1")
raw_port = os.getenv("DB_PORT", "3306")
raw_db   = os.getenv("DB_NAME", "vl_lims")

# Force TCP: never allow 'localhost' (which may use UNIX socket)
host = "127.0.0.1" if raw_host.strip().lower() == "localhost" else raw_host.strip()

# URL-encode for SQLAlchemy URL (encodes @, !, etc.)
user = quote_plus(raw_user)
pwd  = quote_plus(raw_pwd)

# Build URL
DATABASE_URL = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{raw_port}/{raw_db}"

# Helpful boot log (no password)
print(f"[DB] url=mysql+mysqlconnector://{raw_user}:***@{host}:{raw_port}/{raw_db}", flush=True)

# Engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"connection_timeout": 8},
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
