import json
from sqlalchemy.orm import Session
from models.chain import Chain
from services.db import engine
from datetime import datetime

def export_chain_json(out_path=None, limit=100):
    """
    Export chain metrics as JSON in legacy format (last N records).
    """
    session = Session(engine)
    try:
        q = session.query(Chain).order_by(Chain.timestamp.desc()).limit(limit)
        result = []
        for row in q:
            result.append({
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "staked_tokens": row.staked_tokens,
                "missed_blocks": row.missed_blocks,
                "inflation": row.inflation,
                "apr": row.apr,
                "price": row.price,
                "delegators": row.delegators,
                "annual_provisions": row.annual_provisions,
                "supply": row.supply
            })
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
        return js
    finally:
        session.close() 