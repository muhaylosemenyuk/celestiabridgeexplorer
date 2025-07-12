from sqlalchemy import Column, String, Float, Integer, DateTime
from models.base import Base
from datetime import datetime

class Metric(Base):
    __tablename__ = 'metrics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    instance = Column(String, nullable=False)
    metric_name = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True) 