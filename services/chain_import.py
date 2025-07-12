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
from datetime import datetime


def import_chain_to_db():
    """
    Import chain metrics (legacy: staked_tokens, missed_blocks, inflation, apr, price, delegators, annual_provisions, supply)
    into the database. Delegators = total (legacy logic, sum by all active validators).
    """
    staked_tokens = get_staked_tokens()
    missed_blocks = get_all_missed_blocks()
    annual_provisions = get_annual_provisions()
    supply = get_supply()
    price = get_tia_price()
    inflation = (annual_provisions / supply * 100) if annual_provisions and supply else None
    apr = (annual_provisions / (staked_tokens * 1_000_000) * 100) if annual_provisions and staked_tokens else None
    now = datetime.utcnow()

    # Total delegators (legacy logic)
    stats = get_validators_with_delegators()
    total_delegators = sum(stat["delegators"] for stat in stats)

    session = SessionLocal()
    try:
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
        session.commit()
        print(f"Imported chain metrics at {now} (delegators: {total_delegators})")
    except Exception as e:
        session.rollback()
        print(f"Error importing chain metrics: {e}")
        raise
    finally:
        session.close() 