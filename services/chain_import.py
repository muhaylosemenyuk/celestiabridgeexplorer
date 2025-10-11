from data_sources.api import (
    get_staked_tokens,
    get_all_missed_blocks,
    get_annual_provisions,
    get_supply,
    get_tia_price,
    get_validators_with_delegators
)
from models.chain import Chain
from services.db import SessionLocal
from datetime import datetime, date


def import_chain_to_db():
    """
    Import chain metrics (legacy: staked_tokens, missed_blocks, inflation, apr, price, delegators, annual_provisions, supply)
    into the database. Delegators = total (legacy logic, sum by all active validators).
    
    Updates existing record for today or creates new one if none exists.
    """
    staked_tokens = get_staked_tokens()
    missed_blocks = get_all_missed_blocks()
    annual_provisions = get_annual_provisions()
    supply = get_supply()
    price = get_tia_price()
    inflation = (annual_provisions / supply * 100) if annual_provisions and supply else None
    apr = (annual_provisions / (staked_tokens * 1_000_000) * 100) if annual_provisions and staked_tokens else None
    now = datetime.utcnow()
    today = now.date()

    # Total delegators (legacy logic)
    stats = get_validators_with_delegators()
    total_delegators = sum(stat["delegators"] for stat in stats)

    session = SessionLocal()
    try:
        # Check if record exists for today (simplified - only one record per day)
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = datetime.combine(today, datetime.max.time())
        
        existing_record = session.query(Chain).filter(
            Chain.timestamp >= start_of_day,
            Chain.timestamp <= end_of_day
        ).first()
        
        if existing_record:
            # Update existing record
            existing_record.timestamp = now
            existing_record.staked_tokens = staked_tokens
            existing_record.missed_blocks = missed_blocks
            existing_record.inflation = inflation
            existing_record.apr = apr
            existing_record.price = price
            existing_record.delegators = total_delegators
            existing_record.annual_provisions = annual_provisions
            existing_record.supply = supply
            print(f"Updated chain metrics for {today} at {now} (delegators: {total_delegators})")
        else:
            # Create new record
            chain = Chain(
                timestamp=now,
                staked_tokens=staked_tokens,
                missed_blocks=missed_blocks,
                inflation=inflation,
                apr=apr,
                price=price,
                delegators=total_delegators,
                annual_provisions=annual_provisions,
                supply=supply
            )
            session.add(chain)
            print(f"Created new chain metrics for {today} at {now} (delegators: {total_delegators})")
        
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error importing chain metrics: {e}")
        raise
    finally:
        session.close() 