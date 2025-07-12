from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DB_URL
from models.base import Base

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    """Create all tables in the database."""
    Base.metadata.create_all(bind=engine) 