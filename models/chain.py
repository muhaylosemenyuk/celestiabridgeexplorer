from sqlalchemy import Column, String, Float, Integer, DateTime
from models.base import Base
from datetime import datetime

class Chain(Base):
    __tablename__ = 'chain'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    staked_tokens = Column(Float)
    missed_blocks = Column(Integer)
    inflation = Column(Float)
    apr = Column(Float)
    price = Column(Float) 
    delegators = Column(Integer)
    annual_provisions = Column(Float)
    supply = Column(Float)
