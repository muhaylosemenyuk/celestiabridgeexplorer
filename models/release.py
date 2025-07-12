from sqlalchemy import Column, String, Integer, DateTime
from models.base import Base
from datetime import datetime

class Release(Base):
    __tablename__ = 'releases'
    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String, nullable=False)
    published_at = Column(DateTime)
    announce_str = Column(String)
    deadline_str = Column(String) 