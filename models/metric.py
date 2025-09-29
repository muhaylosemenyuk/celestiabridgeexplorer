from sqlalchemy import Column, String, Float, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from models.base import Base
from datetime import datetime

class Metric(Base):
    __tablename__ = 'metrics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    instance = Column(String, nullable=False)
    metric_name = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Foreign key relationship to nodes table
    node_id = Column(Integer, ForeignKey('nodes.id'), nullable=True, comment="Reference to nodes table")
    
    # Relationship to node
    node = relationship("Node", back_populates="metrics") 