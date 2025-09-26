from sqlalchemy import Column, String, Numeric, Date, DateTime, Index, Integer, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from models.base import Base
from datetime import datetime

class Delegation(Base):
    """
    Model for storing historical delegation data.
    
    Workflow:
    - First collection: save all delegations
    - Subsequent collections: save only changes
    - To get delegation on date: take the last record before that date
    
    Key Fields:
    - delegator_address: Address of the delegator
    - validator_address: Address of the validator (operator_address)
    - amount_tia: Delegation amount in TIA (converted from utia)
    - date: Date of the delegation record
    """
    __tablename__ = 'delegations'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True, comment="Unique delegation ID")
    
    # Delegation identifiers
    delegator_address = Column(String(255), nullable=False, index=True, comment="Delegator address (celestia1...)")
    validator_address = Column(String(255), nullable=False, index=True, comment="Validator operator address (celestiavaloper1...)")
    
    # Delegation data
    amount_tia = Column(Numeric(30, 6), nullable=False, comment="Delegation amount in TIA (converted from utia)")
    
    # Timestamps
    date = Column(Date, nullable=False, index=True, comment="Date of the delegation record")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, comment="Record creation timestamp")
    
    # Latest record flag
    is_latest = Column(Boolean, default=True, nullable=False, index=True, comment="True if this is the latest record for this delegator+validator combination")
    
    # Foreign key relationship to validators table
    validator_id = Column(Integer, ForeignKey('validators.id'), nullable=True, comment="Reference to validators table")
    
    # Relationship to validator
    validator = relationship("Validator", back_populates="delegations")
    
    # Indexes for query optimization
    __table_args__ = (
        Index('idx_delegator_date', 'delegator_address', 'date'),  # Main index for delegation lookup by date
        Index('idx_validator_date', 'validator_address', 'date'),  # For validator-specific queries
        Index('idx_date_amount', 'date', 'amount_tia'),  # For analytics by dates
        Index('idx_delegator_validator', 'delegator_address', 'validator_address'),  # For specific delegation queries
        Index('idx_delegator_created', 'delegator_address', 'created_at'),  # For debugging
    )
    
    def to_dict(self):
        """
        Convert delegation object to dictionary for API responses.
        
        Returns:
            dict: Dictionary representation of the delegation with all fields
        """
        return {
            'id': self.id,
            'delegator_address': self.delegator_address,
            'validator_address': self.validator_address,
            'amount_tia': float(self.amount_tia) if self.amount_tia else 0.0,
            'date': self.date.isoformat() if self.date else None,
            'validator_id': self.validator_id,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    @staticmethod
    def convert_utia_to_tia(utia_amount):
        """
        Convert utia (micro TIA) to TIA.
        
        Args:
            utia_amount (str|int|float): Amount in utia
            
        Returns:
            float: Amount in TIA (with 6 decimal places)
        """
        if utia_amount is None:
            return None
        return float(utia_amount) / 1_000_000
    
    def __repr__(self):
        return f"<Delegation(delegator='{self.delegator_address}', validator='{self.validator_address}', amount_tia={self.amount_tia}, date='{self.date}')>"
