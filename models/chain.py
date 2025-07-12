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

class DelegatorStat(Base):
    __tablename__ = 'delegator_stats'
    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_address = Column(String, index=True, nullable=False)
    moniker = Column(String)
    delegators = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True) 