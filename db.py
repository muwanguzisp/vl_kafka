# db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# ⚡ Adjust with your MySQL credentials
DB_USER = "homestead"
DB_PASSWORD = "secret"
DB_HOST = "192.168.1.144"
DB_PORT = "3306"
DB_NAME = "vl_prod_sync"

# SQLAlchemy connection URL
DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
