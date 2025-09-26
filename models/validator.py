from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, Text, DECIMAL
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.base import Base

class Validator(Base):
    """
    Celestia Validator Model
    
    Represents a validator in the Celestia network with all relevant information
    including staking data, commission rates, uptime metrics, and consensus participation.
    
    Key Fields:
    - tokens: Total staked tokens in TIA (converted from utia)
    - voting_power: Validator's voting power in consensus
    - uptime_percent: Block signing uptime percentage
    - commission_rate: Validator's commission rate (0.0-1.0)
    - status: Validator status (BONDED, UNBONDING, UNBONDED)
    """
    __tablename__ = 'validators'
    
    # Basic identifiers
    id = Column(Integer, primary_key=True, autoincrement=True, comment="Unique validator ID")
    operator_address = Column(String(255), unique=True, nullable=False, index=True, comment="Validator operator address (celestiavaloper1...)")
    consensus_address = Column(String(255), index=True, comment="Validator consensus address (celestiavalcons1...)")
    consensus_pubkey = Column(Text, comment="Validator consensus public key")
    
    # Validator description
    moniker = Column(String(255), comment="Validator display name")
    identity = Column(String(255), comment="Validator identity (keybase, etc.)")
    website = Column(String(500), comment="Validator website URL")
    security_contact = Column(String(500), comment="Security contact information")
    details = Column(Text, comment="Additional validator details")
    
    # Status and state
    jailed = Column(Boolean, default=False, comment="Whether validator is jailed")
    status = Column(String(50), comment="Validator status: BONDED, UNBONDING, UNBONDED")
    
    # Staking information
    tokens = Column(DECIMAL(30, 6), comment="Total staked tokens in TIA (converted from utia)")
    min_self_delegation = Column(DECIMAL(30, 6), comment="Minimum self-delegation requirement in TIA")
    
    # Commission settings
    commission_rate = Column(DECIMAL(20, 18), comment="Current commission rate (0.0-1.0)")
    max_commission_rate = Column(DECIMAL(20, 18), comment="Maximum commission rate (0.0-1.0)")
    max_change_rate = Column(DECIMAL(20, 18), comment="Maximum commission change rate (0.0-1.0)")
    commission_update_time = Column(DateTime, comment="Last commission update timestamp")
    
    # Consensus participation
    voting_power = Column(DECIMAL(30, 0), comment="Validator voting power in consensus")
    proposer_priority = Column(DECIMAL(30, 0), comment="Validator proposer priority")
    
    # Uptime and reliability
    missed_blocks_counter = Column(Integer, default=0, comment="Number of missed blocks in current window")
    uptime_percent = Column(DECIMAL(5, 2), comment="Block signing uptime percentage (0.0-100.0)")
    
    # Delegation statistics (updated automatically after delegation import)
    total_delegations = Column(DECIMAL(30, 6), default=0, comment="Total sum of all delegations in TIA")
    total_delegators = Column(Integer, default=0, comment="Total number of unique delegators")
    
    # Timestamps
    created_at = Column(DateTime, default=func.current_timestamp(), comment="Record creation timestamp")
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp(), comment="Record last update timestamp")
    
    # Relationship to delegations
    delegations = relationship("Delegation", back_populates="validator")
    
    def to_dict(self):
        """
        Convert validator object to dictionary for API responses.
        
        Returns:
            dict: Dictionary representation of the validator with all fields
        """
        return {
            'id': self.id,
            'operator_address': self.operator_address,
            'consensus_address': self.consensus_address,
            'consensus_pubkey': self.consensus_pubkey,
            'moniker': self.moniker,
            'identity': self.identity,
            'website': self.website,
            'security_contact': self.security_contact,
            'details': self.details,
            'jailed': self.jailed,
            'status': self.status,
            'tokens': float(self.tokens) if self.tokens else None,
            'commission_rate': float(self.commission_rate) if self.commission_rate else None,
            'max_commission_rate': float(self.max_commission_rate) if self.max_commission_rate else None,
            'max_change_rate': float(self.max_change_rate) if self.max_change_rate else None,
            'commission_update_time': self.commission_update_time.isoformat() if self.commission_update_time else None,
            'min_self_delegation': float(self.min_self_delegation) if self.min_self_delegation else None,
            'voting_power': float(self.voting_power) if self.voting_power else None,
            'proposer_priority': float(self.proposer_priority) if self.proposer_priority else None,
            'missed_blocks_counter': self.missed_blocks_counter,
            'uptime_percent': float(self.uptime_percent) if self.uptime_percent else None,
            'total_delegations': float(self.total_delegations) if self.total_delegations else 0.0,
            'total_delegators': self.total_delegators,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
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
        return f"<Validator(operator_address='{self.operator_address}', moniker='{self.moniker}')>"
