"""
Service for importing wallet balances from Cosmos API to database.

Workflow:
1. First collection: get all balances and save to DB
2. Subsequent collections: get all balances, but save only changes
3. Optimization: batch processing for speed
"""
import logging
import asyncio
import aiohttp
import time
from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Optional
from sqlalchemy import and_, desc, func

from services.db import SessionLocal
from services.cosmos_api import COSMOS_API_BASE_URL, make_cosmos_request
from services.paginated_aggregator import fetch_and_aggregate_paginated
from models.balance import BalanceHistory
from config import WALLET_BATCH_SIZE

logger = logging.getLogger(__name__)

def import_balances_to_db(limit: Optional[int] = None):
    """
    Import wallet balances from Cosmos API to database.
    Uses delta storage - saves only changed balances.
    
    Args:
        limit: Optional limit for number of addresses to process (for testing)
    """
    logger.info("🚀 Starting balance import process...")
    if limit:
        logger.info(f"🔬 Testing mode: limited to {limit} addresses")
    start_time = datetime.now()
    
    try:
        # Step 1: Get addresses (with limit if specified)
        if limit:
            logger.info(f"📋 Step 1: Getting first {limit} wallet addresses from Cosmos API...")
            addresses = get_addresses_with_limit(limit)
        else:
            logger.info("📋 Step 1: Getting all wallet addresses from Cosmos API...")
            addresses = get_all_addresses()
        
        if not addresses:
            logger.error("❌ Failed to get addresses")
            return {"error": "Failed to get addresses"}
        
        logger.info(f"✅ Retrieved {len(addresses)} addresses")
        
        # Step 2: Get addresses with balances
        logger.info("💰 Step 2: Getting balances for all addresses...")
        addresses_with_balances = asyncio.run(get_addresses_with_balances_async(addresses))
        if not addresses_with_balances:
            logger.error("❌ Failed to get addresses with balances")
            return {"error": "Failed to get addresses with balances"}
        logger.info(f"✅ Retrieved balances for {len(addresses_with_balances)} addresses")
        
        # Step 3: Get previous balances for comparison
        logger.info("🔄 Step 3: Getting previous balances for delta comparison...")
        target_date = date.today()
        previous_balances = get_previous_balances(target_date)
        existing_balances = get_existing_balances_for_date(target_date)
        logger.info(f"✅ Found {len(previous_balances)} previous balances for comparison")
        logger.info(f"✅ Found {len(existing_balances)} existing balances for {target_date}")
        
        # Step 4: Initialize statistics
        stats = {
            "total_addresses": len(addresses_with_balances),
            "processed": 0,
            "new_addresses": 0,
            "changed_balances": 0,
            "unchanged_balances": 0,
            "errors": 0
        }
        logger.info(f"📊 Processing {stats['total_addresses']} addresses in batches...")
        
        # Step 5: Process all addresses at once
        logger.info(f"🔄 Processing all {len(addresses_with_balances)} addresses...")
        
        all_new_balances = []
        affected_addresses = set()  # Track addresses that need is_latest flag update
        
        for i, address_data in enumerate(addresses_with_balances):
            try:
                address = address_data['address']
                current_balance = address_data['balance_tia']
                previous_balance = previous_balances.get(address)
                existing_balance = existing_balances.get(address)
                
                # Check if we need to save or update
                if existing_balance is not None:
                    # Record already exists for this date - update it
                    if abs(float(existing_balance) - float(current_balance)) > 0.000001:  # Compare with precision
                        # Balance changed - update existing record in database
                        session = SessionLocal()
                        try:
                            existing_record = session.query(BalanceHistory).filter(
                                BalanceHistory.address == address,
                                BalanceHistory.date == target_date
                            ).first()
                            
                            if existing_record:
                                existing_record.balance_tia = current_balance
                                existing_record.is_latest = True
                                session.commit()
                                logger.debug(f"🔄 Updated existing balance record: {address}")
                            
                        finally:
                            session.close()
                        
                        stats["changed_balances"] += 1
                        # Track this address for flag update
                        affected_addresses.add(address)
                    else:
                        # Balance unchanged - skip
                        stats["unchanged_balances"] += 1
                elif previous_balance is None:
                    # New address
                    all_new_balances.append(create_balance_record(
                        address, target_date, current_balance
                    ))
                    stats["new_addresses"] += 1
                    # Track this address for flag update
                    affected_addresses.add(address)
                elif abs(previous_balance - current_balance) > 0.000001:  # Compare with precision
                    # Balance changed from previous date
                    all_new_balances.append(create_balance_record(
                        address, target_date, current_balance
                    ))
                    stats["changed_balances"] += 1
                    # Track this address for flag update
                    affected_addresses.add(address)
                else:
                    # Balance unchanged from previous date - don't save
                    stats["unchanged_balances"] += 1
                
                stats["processed"] += 1
                
                # Log progress every 1000 addresses
                if (i + 1) % 1000 == 0:
                    progress_pct = ((i + 1) / len(addresses_with_balances)) * 100
                    logger.info(f"📈 Progress: {i + 1}/{len(addresses_with_balances)} "
                              f"({progress_pct:.1f}%) - {stats['new_addresses']} new, "
                              f"{stats['changed_balances']} changed, {stats['unchanged_balances']} unchanged")
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing address {address_data.get('address', 'unknown')}: {e}")
                stats["errors"] += 1
        
        # Save all new balances in one transaction
        if all_new_balances:
            logger.info(f"💾 Saving {len(all_new_balances)} new balances to database...")
            try:
                session = SessionLocal()
                try:
                    # Create BalanceHistory objects
                    balance_objects = []
                    for balance_data in all_new_balances:
                        balance_obj = BalanceHistory(
                            address=balance_data['address'],
                            date=balance_data['date'],
                            balance_tia=balance_data['balance_tia'],
                            is_latest=True  # New records are latest by default
                        )
                        balance_objects.append(balance_obj)
                    
                    session.add_all(balance_objects)
                    session.commit()
                    logger.info(f"✅ Successfully saved {len(all_new_balances)} new balances to database")
                finally:
                    session.close()
            except Exception as e:
                logger.error(f"❌ Error saving to database: {e}")
                stats["errors"] += len(all_new_balances)
        
        # Step 6: Update is_latest flags for affected addresses
        if affected_addresses:
            logger.info(f"🔄 Step 6: Updating is_latest flags for {len(affected_addresses)} affected addresses...")
            update_latest_balance_flags(affected_addresses)
        else:
            logger.info("✅ Step 6: No addresses need is_latest flag update")
        
        # Final statistics
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"🎉 Import completed successfully!")
        logger.info(f"⏱️ Total time: {duration}")
        logger.info(f"📊 Final statistics: {stats}")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error during import: {e}")
        return {"error": str(e)}

def get_addresses_with_limit(limit: int) -> List[str]:
    """
    Get limited number of wallet addresses from Cosmos API.
    More efficient than getting all addresses and then limiting.
    """
    logger.info(f"🔍 Fetching first {limit} wallet addresses from Cosmos API...")
    
    try:
        # Use direct API call with pagination limit
        logger.info("📡 Making request to /cosmos/auth/v1beta1/accounts...")
        response = make_cosmos_request(
            "/cosmos/auth/v1beta1/accounts",
            {"pagination.limit": limit}
        )
        
        if not response or 'accounts' not in response:
            logger.error("❌ No accounts found in API response")
            return []
        
        accounts = response['accounts']
        logger.info(f"📋 Processing {len(accounts)} account records...")
        
        # Extract addresses
        address_list = [acc['address'] for acc in accounts if 'address' in acc]
        logger.info(f"✅ Successfully extracted {len(address_list)} addresses (limited to {limit})")
        return address_list
        
    except Exception as e:
        logger.error(f"❌ Error getting addresses with limit: {e}")
        return []

def get_all_addresses() -> List[str]:
    """
    Get all wallet addresses from Cosmos API.
    Uses paginated_aggregator to get all addresses.
    """
    logger.info("🔍 Fetching all wallet addresses from Cosmos API...")
    
    try:
        # Get all addresses using paginated aggregator
        logger.info("📡 Making request to /cosmos/auth/v1beta1/accounts...")
        accounts = fetch_and_aggregate_paginated(
            "/cosmos/auth/v1beta1/accounts",
            {"pagination.limit": 100000},
            "accounts",
            aggregate="all"
        )
        
        if not accounts:
            logger.error("❌ No accounts found in API response")
            return []
        
        # Extract addresses
        logger.info(f"📋 Processing {len(accounts)} account records...")
        address_list = [acc['address'] for acc in accounts if 'address' in acc]
        logger.info(f"✅ Successfully extracted {len(address_list)} addresses")
        return address_list
        
    except Exception as e:
        logger.error(f"❌ Error getting addresses: {e}")
        return []

async def get_balance_async(session: aiohttp.ClientSession, address: str) -> Dict[str, float]:
    """
    Get balance for a single address asynchronously.
    """
    try:
        url = f"{COSMOS_API_BASE_URL}/cosmos/bank/v1beta1/balances/{address}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                data = await response.json()
                if data and 'balances' in data:
                    for balance in data['balances']:
                        if balance.get('denom') == 'utia':
                            utia_amount = int(balance.get('amount', 0))
                            return {
                                'address': address,
                                'balance_tia': utia_amount / 1000000.0
                            }
                return {'address': address, 'balance_tia': 0.0}
            else:
                return {'address': address, 'balance_tia': 0.0}
    except Exception as e:
        logger.warning(f"⚠️ Error getting balance for {address}: {e}")
        return {'address': address, 'balance_tia': 0.0}

async def get_addresses_with_balances_async(addresses: List[str], batch_size: int = WALLET_BATCH_SIZE) -> List[Dict[str, float]]:
    """
    Get addresses with their balances using async requests.
    
    Args:
        addresses: List of addresses to get balances for
        batch_size: Number of concurrent requests
        
    Returns:
        List of dictionaries with 'address' and 'balance_tia' keys
    """
    logger.info(f"💰 Getting balances for {len(addresses)} addresses (async, batch_size={batch_size})...")
    start_time = time.time()
    
    addresses_with_balances = []
    
    # Process in batches to avoid overwhelming the API
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        batch_start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            tasks = [get_balance_async(session, addr) for addr in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in batch_results:
                if isinstance(result, dict):
                    addresses_with_balances.append(result)
                else:
                    # Handle exceptions - skip invalid results instead of adding 'unknown'
                    logger.warning(f"⚠️ Exception in batch: {result}")
                    # Don't add 'unknown' records to avoid polluting the database
        
        # Log batch progress
        batch_duration = time.time() - batch_start_time
        batch_rate = len(batch) / batch_duration if batch_duration > 0 else 0
        total_processed = min(i + batch_size, len(addresses))
        
        logger.info(f"📊 Batch {i//batch_size + 1}: {total_processed}/{len(addresses)} addresses "
                   f"({total_processed/len(addresses)*100:.1f}%) - "
                   f"Batch rate: {batch_rate:.1f} addr/sec")
    
    total_duration = time.time() - start_time
    total_rate = len(addresses) / total_duration if total_duration > 0 else 0
    
    logger.info(f"✅ Retrieved balances for {len(addresses_with_balances)} addresses")
    logger.info(f"⏱️ Total time: {total_duration:.1f}s - Rate: {total_rate:.1f} addr/sec")
    
    return addresses_with_balances



def get_previous_balances(target_date: date) -> Dict[str, float]:
    """
    Get the latest balances for each address before target_date for comparison.
    """
    logger.info(f"🔍 Looking for latest balances before {target_date}")
    
    session = SessionLocal()
    try:
        # Get the latest balance for each address before target_date
        # Subquery to get the latest date for each address before target_date
        latest_dates = session.query(
            BalanceHistory.address,
            func.max(BalanceHistory.date).label('latest_date')
        ).filter(
            BalanceHistory.date < target_date
        ).group_by(BalanceHistory.address).subquery()
        
        # Get the actual balances for those latest dates
        balances = session.query(BalanceHistory).join(
            latest_dates,
            and_(
                BalanceHistory.address == latest_dates.c.address,
                BalanceHistory.date == latest_dates.c.latest_date
            )
        ).all()
        
        result = {balance.address: float(balance.balance_tia) for balance in balances}
        logger.info(f"📊 Found {len(result)} previous balances for comparison")
        return result
    finally:
        session.close()

def get_existing_balances_for_date(target_date: date) -> Dict[str, float]:
    """
    Get balances that already exist for the target_date to avoid duplicates.
    """
    logger.info(f"🔍 Checking for existing balances on {target_date}")
    
    session = SessionLocal()
    try:
        balances = session.query(BalanceHistory).filter(
            BalanceHistory.date == target_date
        ).all()
        
        result = {balance.address: float(balance.balance_tia) for balance in balances}
        logger.info(f"📊 Found {len(result)} existing balances for {target_date}")
        return result
    finally:
        session.close()


def create_balance_record(address: str, target_date: date, balance_tia: float) -> Dict:
    """
    Create balance record for saving.
    """
    # Validate address - skip invalid addresses
    if not address or address == 'unknown' or not isinstance(address, str):
        raise ValueError(f"Invalid address: {address}")
    
    # Validate balance - ensure it's a valid number
    if balance_tia is None or not isinstance(balance_tia, (int, float)):
        raise ValueError(f"Invalid balance: {balance_tia}")
    
    return {
        'address': address,
        'date': target_date,
        'balance_tia': Decimal(str(balance_tia)),
    }

def update_latest_balance_flags(affected_addresses: set):
    """
    Update is_latest flags for balance records.
    Sets is_latest=False for affected addresses that are NOT from today.
    New records already have is_latest=True by default.
    
    Args:
        affected_addresses: Set of addresses to update.
    """
    logger.info("🔄 Updating is_latest flags for balance records...")
    
    try:
        session = SessionLocal()
        try:
            # Update only affected addresses
            logger.info(f"📊 Updating flags for {len(affected_addresses)} affected addresses...")
            
            # Set False for affected addresses that are NOT from latest date
            latest_date = session.query(func.max(BalanceHistory.date)).scalar()
            for address in affected_addresses:
                session.query(BalanceHistory).filter(
                    and_(
                        BalanceHistory.address == address,
                        BalanceHistory.date != latest_date
                    )
                ).update({'is_latest': False})
            
            logger.info(f"✅ Set is_latest=False for non-latest records in {len(affected_addresses)} addresses")
            
            session.commit()
            logger.info(f"✅ Updated is_latest flags for {len(affected_addresses)} addresses")
                
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Error updating is_latest flags: {e}")
        raise

def get_import_progress() -> Dict:
    """
    Return import progress information.
    """
    try:
        session = SessionLocal()
        try:
            # Last import date
            last_import = session.query(BalanceHistory).order_by(
                desc(BalanceHistory.date)
            ).first()
            
            if not last_import:
                return {"status": "no_data", "message": "Import has not been performed yet"}
            
            # Number of records for last date
            last_date_count = session.query(BalanceHistory).filter(
                BalanceHistory.date == last_import.date
            ).count()
            
            # Total number of unique addresses
            total_addresses = session.query(BalanceHistory.address).distinct().count()
            
            return {
                "status": "success",
                "last_import_date": last_import.date.isoformat(),
                "last_import_count": last_date_count,
                "total_unique_addresses": total_addresses,
                "total_records": session.query(BalanceHistory).count()
            }
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"Error getting progress: {e}")
        return {"status": "error", "message": str(e)}
