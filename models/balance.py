from sqlalchemy import Column, String, Numeric, Date, DateTime, Index, Integer
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
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Indexes for query optimization
    __table_args__ = (
        Index('idx_address_date', 'address', 'date'),  # Main index for balance lookup by date
        Index('idx_date_balance', 'date', 'balance_tia'),  # For analytics by dates
        Index('idx_address_created', 'address', 'created_at'),  # For debugging
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
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
