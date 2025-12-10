# db.py
import os
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Be explicit about the .env path used by systemd WorkingDirectory
ENV_PATH = Path.cwd() / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# ───────────────────────────────────────────────
# VL LIMS connection (default)
# ───────────────────────────────────────────────

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
VL_DB_URL = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{raw_port}/{raw_db}"

# Helpful boot log (no password)
print(f"[DB] url=mysql+mysqlconnector://{raw_user}:***@{host}:{raw_port}/{raw_db}", flush=True)

# Engine
vl_engine = create_engine(
    VL_DB_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"connection_timeout": 8},
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=vl_engine)


# ───────────────────────────────────────────────
# EID LIMS connection (separate database)
# ───────────────────────────────────────────────
eid_user = os.getenv("EID_DB_USER", raw_user)
eid_pwd  = os.getenv("EID_DB_PASSWORD", raw_pwd)
eid_host = os.getenv("EID_DB_HOST", host)
eid_port = os.getenv("EID_DB_PORT", raw_port)
eid_name = os.getenv("EID_DB_NAME", "eid_lims")

eid_user_q = quote_plus(eid_user)
eid_pwd_q  = quote_plus(eid_pwd)

EID_DB_URL = f"mysql+mysqlconnector://{eid_user_q}:{eid_pwd_q}@{eid_host}:{eid_port}/{eid_name}"
print(f"[DB] EID_LIMS → mysql+mysqlconnector://{eid_user}:***@{eid_host}:{eid_port}/{eid_name}", flush=True)

eid_engine = create_engine(
    EID_DB_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"connection_timeout": 8},
)
EIDSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=eid_engine)


# ───────────────────────────────────────────────
# Session helpers
# ───────────────────────────────────────────────
def get_session():
    """
    Return a new SQLAlchemy Session bound to the VL LIMS database.
    Used by Kafka consumers and other background workers.
    """
    return SessionLocal()


def get_eid_session():
    """Return a new SQLAlchemy Session bound to the EID LIMS database."""
    return EIDSessionLocal()
