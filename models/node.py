from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.orm import relationship
from models.base import Base

class Node(Base):
    __tablename__ = 'nodes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    peer_id = Column(String, unique=True, nullable=False)
    ip = Column(String)
    city = Column(String)
    region = Column(String)
    country = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    org = Column(String)
    
    # Relationship to metrics
    metrics = relationship("Metric", back_populates="node") 