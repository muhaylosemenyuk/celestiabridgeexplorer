from sqlalchemy import Column, String, Numeric, Date, DateTime, Index, Integer, Boolean
from models.base import Base
from datetime import datetime

class BalanceHistory(Base):
    """
    Model for storing historical wallet balances.
    
    Workflow:
    - First collection: save all balances
    - Subsequent collections: save only changes
    - To get balance on date: take the last record before that date
    """
    __tablename__ = 'balance_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    address = Column(String(255), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    balance_tia = Column(Numeric(20, 6), nullable=False, default=0.0)
    
    # Latest record flag
    is_latest = Column(Boolean, default=True, nullable=False, index=True, comment="True if this is the latest record for this address")
    
    # Indexes for query optimization
    __table_args__ = (
        Index('idx_address_date', 'address', 'date'),  # Main index for balance lookup by date
        Index('idx_date_balance', 'date', 'balance_tia'),  # For analytics by dates
        Index('idx_address_latest', 'address', 'is_latest'),  # For latest record queries
    )
    
    def __repr__(self):
        return f"<BalanceHistory(address='{self.address}', date='{self.date}', balance_tia={self.balance_tia})>"
    
    def to_dict(self):
        """Convert object to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'address': self.address,
            'date': self.date.isoformat() if self.date else None,
            'balance_tia': float(self.balance_tia) if self.balance_tia else 0.0,
            'is_latest': self.is_latest
        }
