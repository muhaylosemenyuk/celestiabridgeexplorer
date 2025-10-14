from sqlalchemy import Column, String, Float, Integer, Boolean
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
    provider = Column(String)
    
    # New fields from location.json
    continent = Column(String)
    updated_at = Column(String)  # Keep as string for simplicity
    
    # Rules fields from score_breakdown.rules
    city_over_limit = Column(Boolean)
    country_over_limit = Column(Boolean)
    continent_over_limit = Column(Boolean)
    provider_over_limit = Column(Boolean)
    provider_hetzner = Column(Boolean)
    
    # Relationship to metrics
    metrics = relationship("Metric", back_populates="node") 