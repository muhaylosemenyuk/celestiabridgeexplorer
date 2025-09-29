"""
Service for exporting wallet balances in JSON format.

Used for API endpoints and MCP integration.
"""
import json
import logging
from datetime import date, timedelta
from sqlalchemy import and_, desc, func

from services.db import SessionLocal
from services.balance_import import get_import_progress
from models.balance import BalanceHistory

logger = logging.getLogger(__name__)

def export_balance_on_date_json(address: str, target_date: date, out_path=None) -> str:
    """
    Export balance of address on date in JSON format.
    
    Args:
        address: Wallet address
        target_date: Date for which balance is needed
        out_path: Optional path to save JSON file
        
    Returns:
        JSON string with balance information
    """
    try:
        session = SessionLocal()
        try:
            balance = session.query(BalanceHistory).filter(
                and_(
                    BalanceHistory.address == address,
                    BalanceHistory.date <= target_date
                )
            ).order_by(desc(BalanceHistory.date)).first()
            
            if balance:
                result = {
                    'address': balance.address,
                    'date': balance.date.isoformat(),
                    'balance_tia': float(balance.balance_tia),
                    'record_date': balance.date.isoformat(),
                }
            else:
                result = {
                    "error": "Balance not found",
                    "address": address,
                    "date": target_date.isoformat()
                }
        finally:
            session.close()
        
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js
            
    except Exception as e:
        logger.error(f"Error exporting balance: {e}")
        result = {
            "error": str(e),
            "address": address,
            "date": target_date.isoformat()
        }
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js

def export_top_wallets_json(limit: int = 100, target_date: date = None, out_path=None) -> str:
    """
    Export top wallets in JSON format.
    
    Args:
        limit: Number of wallets to return
        target_date: Date for ranking (default - latest available)
        out_path: Optional path to save JSON file
        
    Returns:
        JSON string with list of top wallets
    """
    try:
        session = SessionLocal()
        try:
            # If date not specified, take latest available
            if target_date is None:
                latest_date = session.query(func.max(BalanceHistory.date)).scalar()
                if not latest_date:
                    result = {"error": "No data available"}
                else:
                    target_date = latest_date
            
            if target_date:
                # Get latest balances for each address up to target_date
                subquery = session.query(
                    BalanceHistory.address,
                    func.max(BalanceHistory.date).label('max_date')
                ).filter(
                    BalanceHistory.date <= target_date
                ).group_by(BalanceHistory.address).subquery()
                
                # Join with main data
                top_wallets = session.query(BalanceHistory).join(
                    subquery,
                    and_(
                        BalanceHistory.address == subquery.c.address,
                        BalanceHistory.date == subquery.c.max_date
                    )
                ).order_by(desc(BalanceHistory.balance_tia)).limit(limit).all()
                
                wallets = [
                    {
                        'address': wallet.address,
                        'balance_tia': float(wallet.balance_tia),
                        'date': wallet.date.isoformat(),
                        'rank': i + 1
                    }
                    for i, wallet in enumerate(top_wallets)
                ]
                
                result = {
                    "success": True,
                    "count": len(wallets),
                    "limit": limit,
                    "date": target_date.isoformat(),
                    "wallets": wallets
                }
            else:
                result = {"error": "No data available"}
        finally:
            session.close()
        
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js
        
    except Exception as e:
        logger.error(f"Error exporting top wallets: {e}")
        result = {
            "error": str(e),
            "limit": limit,
            "date": target_date.isoformat() if target_date else "latest"
        }
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js

def export_balance_history_json(address: str, days: int = 30, out_path=None) -> str:
    """
    Export balance history of address in JSON format.
    
    Args:
        address: Wallet address
        days: Number of days for history
        out_path: Optional path to save JSON file
        
    Returns:
        JSON string with balance history
    """
    try:
        session = SessionLocal()
        try:
            start_date = date.today() - timedelta(days=days)
            
            history = session.query(BalanceHistory).filter(
                and_(
                    BalanceHistory.address == address,
                    BalanceHistory.date >= start_date
                )
            ).order_by(BalanceHistory.date).all()
            
            history_data = [
                {
                    'date': record.date.isoformat(),
                    'balance_tia': float(record.balance_tia)
                }
                for record in history
            ]
            
            result = {
                "success": True,
                "address": address,
                "days": days,
                "count": len(history_data),
                "history": history_data
            }
        finally:
            session.close()
        
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js
        
    except Exception as e:
        logger.error(f"Error exporting balance history: {e}")
        result = {
            "error": str(e),
            "address": address,
            "days": days
        }
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js

def export_balance_stats_json(target_date: date = None, out_path=None) -> str:
    """
    Export balance statistics in JSON format.
    
    Args:
        target_date: Date for statistics (default - latest)
        out_path: Optional path to save JSON file
        
    Returns:
        JSON string with statistics
    """
    try:
        session = SessionLocal()
        try:
            # If date not specified, take latest available
            if target_date is None:
                latest_date = session.query(func.max(BalanceHistory.date)).scalar()
                if not latest_date:
                    result = {"error": "No data available"}
                else:
                    target_date = latest_date
            
            if target_date:
                # Get latest balances for each address up to target_date
                subquery = session.query(
                    BalanceHistory.address,
                    func.max(BalanceHistory.date).label('max_date')
                ).filter(
                    BalanceHistory.date <= target_date
                ).group_by(BalanceHistory.address).subquery()
                
                # Statistics
                stats_query = session.query(
                    func.count(BalanceHistory.address).label('total_addresses'),
                    func.sum(BalanceHistory.balance_tia).label('total_balance_tia'),
                    func.avg(BalanceHistory.balance_tia).label('avg_balance_tia'),
                    func.max(BalanceHistory.balance_tia).label('max_balance_tia'),
                    func.min(BalanceHistory.balance_tia).label('min_balance_tia')
                ).join(
                    subquery,
                    and_(
                        BalanceHistory.address == subquery.c.address,
                        BalanceHistory.date == subquery.c.max_date
                    )
                ).first()
                
                if stats_query:
                    stats = {
                        'date': target_date.isoformat(),
                        'total_addresses': stats_query.total_addresses or 0,
                        'total_balance_tia': float(stats_query.total_balance_tia or 0),
                        'avg_balance_tia': float(stats_query.avg_balance_tia or 0),
                        'max_balance_tia': float(stats_query.max_balance_tia or 0),
                        'min_balance_tia': float(stats_query.min_balance_tia or 0)
                    }
                    result = {
                        "success": True,
                        "stats": stats
                    }
                else:
                    result = {"error": "No data available for statistics"}
            else:
                result = {"error": "No data available"}
        finally:
            session.close()
        
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js
        
    except Exception as e:
        logger.error(f"Error exporting statistics: {e}")
        result = {
            "error": str(e),
            "date": target_date.isoformat() if target_date else "latest"
        }
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js

def export_wallet_search_json(query: str, limit: int = 50, out_path=None) -> str:
    """
    Export wallet search results in JSON format.
    
    Args:
        query: Search query
        limit: Maximum number of results
        out_path: Optional path to save JSON file
        
    Returns:
        JSON string with search results
    """
    try:
        session = SessionLocal()
        try:
            # Get latest balances for found addresses
            latest_date = session.query(func.max(BalanceHistory.date)).scalar()
            if not latest_date:
                result = {"error": "No data available"}
            else:
                subquery = session.query(
                    BalanceHistory.address,
                    func.max(BalanceHistory.date).label('max_date')
                ).filter(
                    and_(
                        BalanceHistory.address.like(f'%{query}%'),
                        BalanceHistory.date <= latest_date
                    )
                ).group_by(BalanceHistory.address).subquery()
                
                wallets = session.query(BalanceHistory).join(
                    subquery,
                    and_(
                        BalanceHistory.address == subquery.c.address,
                        BalanceHistory.date == subquery.c.max_date
                    )
                ).order_by(desc(BalanceHistory.balance_tia)).limit(limit).all()
                
                wallets_data = [
                    {
                        'address': wallet.address,
                        'balance_tia': float(wallet.balance_tia),
                        'date': wallet.date.isoformat()
                    }
                    for wallet in wallets
                ]
                
                result = {
                    "success": True,
                    "query": query,
                    "count": len(wallets_data),
                    "limit": limit,
                    "wallets": wallets_data
                }
        finally:
            session.close()
        
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js
        
    except Exception as e:
        logger.error(f"Error exporting search: {e}")
        result = {
            "error": str(e),
            "query": query,
            "limit": limit
        }
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js

def export_balance_summary_json(out_path=None) -> str:
    """
    Export general information about balance system.
    
    Args:
        out_path: Optional path to save JSON file
        
    Returns:
        JSON string with general information
    """
    try:
        # Get import progress
        import_progress = get_import_progress()
        
        # Get statistics
        stats_result = export_balance_stats_json()
        balance_stats = json.loads(stats_result)
        
        result = {
            "success": True,
            "import_progress": import_progress,
            "balance_stats": balance_stats,
            "api_endpoints": {
                "get_balance": "/balances/{address}?date=YYYY-MM-DD",
                "get_top_wallets": "/balances/top?limit=100&date=YYYY-MM-DD",
                "get_balance_history": "/balances/{address}/history?days=30",
                "get_balance_stats": "/balances/stats?date=YYYY-MM-DD",
                "search_wallets": "/balances/search?q={query}&limit=50"
            }
        }
        
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js
        
    except Exception as e:
        logger.error(f"Error exporting general information: {e}")
        result = {
            "error": str(e)
        }
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js