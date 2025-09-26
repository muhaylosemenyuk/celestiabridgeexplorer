"""
Service for importing delegation data from Cosmos API to database.

Workflow:
1. First collection: get all delegations and save to DB
2. Subsequent collections: get all delegations, but save only changes
3. Optimization: batch processing for speed
"""
import logging
import time
from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from sqlalchemy import and_, desc, func
from concurrent.futures import ThreadPoolExecutor, as_completed

from services.db import SessionLocal
from models.delegation import Delegation
from models.validator import Validator
from config import WALLET_BATCH_SIZE

logger = logging.getLogger(__name__)

def import_delegations_to_db(limit: Optional[int] = None):
    """
    Import delegation data from Cosmos API to database.
    Uses delta storage - saves only changed delegations.
    
    Args:
        limit: Optional limit for number of validators to process (for testing)
    """
    logger.info("🚀 Starting delegation import process...")
    if limit:
        logger.info(f"🔬 Testing mode: limited to {limit} validators")
    start_time = datetime.now()
    
    try:
        # Step 1: Get all validators
        logger.info("📋 Step 1: Getting all validators from database...")
        validators = get_all_validators_from_db()
        
        if not validators:
            logger.error("❌ No validators found in database")
            return {"error": "No validators found in database"}
        
        if limit:
            validators = validators[:limit]
        
        logger.info(f"✅ Retrieved {len(validators)} validators")
        
        # Step 2: Get all delegations for all validators
        logger.info("💰 Step 2: Getting delegations for all validators...")
        all_delegations = get_all_delegations_for_validators(validators)
        if not all_delegations:
            logger.error("❌ Failed to get delegations")
            return {"error": "Failed to get delegations"}
        logger.info(f"✅ Retrieved {len(all_delegations)} delegations")
        
        # Step 3: Get previous delegations for delta comparison
        logger.info("🔄 Step 3: Getting previous delegations for delta comparison...")
        target_date = date.today()
        previous_delegations = get_previous_delegations(target_date)
        existing_delegations = get_existing_delegations_for_date(target_date)
        logger.info(f"✅ Found {len(previous_delegations)} previous delegations for comparison")
        logger.info(f"✅ Found {len(existing_delegations)} existing delegations for {target_date}")
        
        # Step 4: Initialize statistics
        stats = {
            "total_delegations": len(all_delegations),
            "processed": 0,
            "new_delegations": 0,
            "changed_delegations": 0,
            "unchanged_delegations": 0,
            "deleted_delegations": 0,
            "disappeared_delegations": 0,
            "errors": 0
        }
        logger.info(f"📊 Processing {stats['total_delegations']} delegations...")
        
        # Step 5: Process all delegations
        logger.info(f"🔄 Processing all {len(all_delegations)} delegations...")
        
        all_new_delegations = []
        updates_to_process = []  # Batch updates for efficiency
        affected_combinations = set()  # Track combinations that need is_latest flag update
        total_delegations = len(all_delegations)
        
        for i, delegation_data in enumerate(all_delegations):
            try:
                delegator_address = delegation_data['delegator_address']
                validator_address = delegation_data['validator_address']
                current_amount = delegation_data['amount_tia']
                
                # Create key for comparison
                delegation_key = f"{delegator_address}_{validator_address}"
                previous_amount = previous_delegations.get(delegation_key)
                existing_amount = existing_delegations.get(delegation_key)
                
                # Check if we need to save
                if existing_amount is not None:
                    # Record already exists for this date - update it
                    if abs(float(existing_amount) - float(current_amount)) > 0.000001:  # Compare with precision
                        # Amount changed - update existing record in database
                        session = SessionLocal()
                        try:
                            existing_record = session.query(Delegation).filter(
                                Delegation.delegator_address == delegator_address,
                                Delegation.validator_address == validator_address,
                                Delegation.date == target_date
                            ).first()
                            
                            if existing_record:
                                existing_record.amount_tia = current_amount
                                existing_record.created_at = datetime.utcnow()
                                existing_record.is_latest = True
                                session.commit()
                                logger.debug(f"🔄 Updated existing record: {delegator_address} -> {validator_address}")
                            
                        finally:
                            session.close()
                        
                        stats["changed_delegations"] += 1
                        # Track this combination for flag update
                        affected_combinations.add(f"{delegator_address}|{validator_address}")
                    else:
                        # Amount unchanged - skip
                        stats["unchanged_delegations"] += 1
                elif previous_amount is None:
                    # New delegation (no previous record)
                    all_new_delegations.append(create_delegation_record(
                        delegation_data, target_date
                    ))
                    stats["new_delegations"] += 1
                    # Track this combination for flag update
                    affected_combinations.add(f"{delegator_address}|{validator_address}")
                elif abs(float(previous_amount) - float(current_amount)) > 0.000001:  # Compare with precision
                    # Delegation changed from previous date
                    all_new_delegations.append(create_delegation_record(
                        delegation_data, target_date
                    ))
                    stats["changed_delegations"] += 1
                    # Track this combination for flag update
                    affected_combinations.add(f"{delegator_address}|{validator_address}")
                else:
                    # Delegation unchanged from previous date - don't save
                    stats["unchanged_delegations"] += 1
                
                stats["processed"] += 1
                
                # Show progress every 50,000 records or at the end
                if (i + 1) % 50000 == 0 or (i + 1) == total_delegations:
                    progress_percent = ((i + 1) / total_delegations) * 100
                    logger.info(f"📊 Progress: {i + 1:,}/{total_delegations:,} ({progress_percent:.1f}%) - "
                              f"New: {stats['new_delegations']:,}, Changed: {stats['changed_delegations']:,}, "
                              f"Unchanged: {stats['unchanged_delegations']:,}")
                
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing delegation {delegation_data.get('delegator_address', 'unknown')}: {e}")
                stats["errors"] += 1
        
        # Step 6: Handle deleted delegations (set to zero)
        logger.info("🔄 Step 6: Processing deleted delegations...")
        deleted_count, deleted_combinations = process_deleted_delegations(previous_delegations, all_delegations, target_date)
        stats["deleted_delegations"] = deleted_count
        affected_combinations.update(deleted_combinations)  # Add deleted combinations to affected set
        logger.info(f"✅ Processed {deleted_count} deleted delegations")
        
        # Step 7: Handle delegations that exist in DB but not in current API response
        logger.info("🔄 Step 7: Processing delegations that disappeared from API...")
        disappeared_count = process_disappeared_delegations(all_delegations, target_date)
        stats["disappeared_delegations"] = disappeared_count
        logger.info(f"✅ Processed {disappeared_count} disappeared delegations")
        
        # Save all new delegations in one transaction
        if all_new_delegations:
            logger.info(f"💾 Saving {len(all_new_delegations)} new delegations to database...")
            try:
                session = SessionLocal()
                try:
                    # Create Delegation objects
                    delegation_objects = []
                    for delegation_data in all_new_delegations:
                        delegation_obj = Delegation(
                            delegator_address=delegation_data['delegator_address'],
                            validator_address=delegation_data['validator_address'],
                            amount_tia=delegation_data['amount_tia'],
                            date=delegation_data['date'],
                            validator_id=delegation_data.get('validator_id'),
                            created_at=delegation_data['created_at'],
                            is_latest=True  # New records are latest by default
                        )
                        delegation_objects.append(delegation_obj)
                    
                    session.add_all(delegation_objects)
                    session.commit()
                    logger.info(f"✅ Successfully saved {len(all_new_delegations)} new delegations to database")
                finally:
                    session.close()
            except Exception as e:
                logger.error(f"❌ Error saving to database: {e}")
                stats["errors"] += len(all_new_delegations)
        
        # Step 8: Update is_latest flags for affected combinations
        if affected_combinations:
            logger.info(f"🔄 Step 8: Updating is_latest flags for {len(affected_combinations)} affected combinations...")
            update_latest_delegation_flags(affected_combinations)
        else:
            logger.info("✅ Step 8: No combinations need is_latest flag update")
        
        # Step 9: Update validator statistics with delegation data
        logger.info("📊 Step 9: Updating validator statistics with delegation data...")
        update_validator_delegation_stats()
        
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

def get_all_validators_from_db() -> List[Dict[str, str]]:
    """
    Get all validators from database.
    """
    logger.info("🔍 Fetching all validators from database...")
    
    session = SessionLocal()
    try:
        validators = session.query(Validator).filter(
            Validator.operator_address.isnot(None)
        ).all()
        
        validator_list = [
            {
                'operator_address': v.operator_address,
                'id': v.id
            }
            for v in validators
        ]
        
        logger.info(f"✅ Retrieved {len(validator_list)} validators from database")
        return validator_list
        
    except Exception as e:
        logger.error(f"❌ Error getting validators from database: {e}")
        return []
    finally:
        session.close()

def get_all_delegations_for_validators(validators: List[Dict[str, str]]) -> List[Dict]:
    """
    Get all delegations for all validators using parallel processing.
    """
    logger.info(f"💰 Getting delegations for {len(validators)} validators...")
    start_time = time.time()
    
    all_delegations = []
    
    # Process validators in batches
    batch_size = min(WALLET_BATCH_SIZE, len(validators))
    
    total_batches = (len(validators) + batch_size - 1) // batch_size
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_num = i//batch_size + 1
        logger.info(f"📊 Processing batch {batch_num}/{total_batches} ({len(batch)} validators)")
        
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Create tasks for each validator in batch
            future_to_validator = {
                executor.submit(get_delegations_for_validator, validator): validator 
                for validator in batch
            }
            
            # Process results as they complete
            completed_count = 0
            for future in as_completed(future_to_validator):
                validator = future_to_validator[future]
                try:
                    delegations = future.result()
                    if delegations:
                        all_delegations.extend(delegations)
                except Exception as e:
                    logger.warning(f"⚠️ Error getting delegations for validator {validator.get('operator_address', 'unknown')}: {e}")
                
                completed_count += 1
                if completed_count % 50 == 0 or completed_count == len(batch):
                    logger.info(f"📈 Batch {batch_num}/{total_batches}: {completed_count}/{len(batch)} validators processed")
    
    total_duration = time.time() - start_time
    total_rate = len(validators) / total_duration if total_duration > 0 else 0
    
    logger.info(f"✅ Retrieved {len(all_delegations)} delegations from {len(validators)} validators")
    logger.info(f"⏱️ Total time: {total_duration:.1f}s - Rate: {total_rate:.1f} validators/sec")
    
    return all_delegations

def get_delegations_for_validator(validator: Dict[str, str]) -> List[Dict]:
    """
    Get delegations for a single validator.
    """
    validator_address = validator['operator_address']
    validator_id = validator['id']
    
    try:
        # Get delegations using direct API call with high limit
        from services.cosmos_api import make_cosmos_request
        
        endpoint = f"/cosmos/staking/v1beta1/validators/{validator_address}/delegations"
        params = {"pagination.limit": 100000}  # Same limit as original get_validator_delegations
        
        delegations_data = make_cosmos_request(endpoint, params)
        if not delegations_data or 'delegation_responses' not in delegations_data:
            return []

        delegations = delegations_data['delegation_responses']
        
        if not delegations:
            return []
        
        # Process delegations
        processed_delegations = []
        for delegation in delegations:
            try:
                delegation_data = delegation.get('delegation', {})
                balance_data = delegation.get('balance', {})
                
                delegator_address = delegation_data.get('delegator_address')
                amount_utia = balance_data.get('amount')
                
                if not delegator_address or not amount_utia:
                    continue
                
                # Convert utia to TIA
                amount_tia = Delegation.convert_utia_to_tia(amount_utia)
                
                processed_delegations.append({
                    'delegator_address': delegator_address,
                    'validator_address': validator_address,
                    'amount_tia': Decimal(str(amount_tia)),
                    'validator_id': validator_id
                })
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing delegation: {e}")
                continue
        
        return processed_delegations
        
    except Exception as e:
        logger.warning(f"⚠️ Error getting delegations for validator {validator_address}: {e}")
        return []

def get_previous_delegations(target_date: date) -> Dict[str, float]:
    """
    Get the latest delegations for each delegator-validator pair before target_date for comparison.
    """
    logger.info(f"🔍 Looking for latest delegations before {target_date}")
    
    session = SessionLocal()
    try:
        # Get the latest delegation for each delegator-validator pair before target_date
        # Subquery to get the latest date for each pair before target_date
        latest_dates = session.query(
            Delegation.delegator_address,
            Delegation.validator_address,
            func.max(Delegation.date).label('latest_date')
        ).filter(
            Delegation.date < target_date
        ).group_by(Delegation.delegator_address, Delegation.validator_address).subquery()
        
        # Get the actual delegations for those latest dates
        delegations = session.query(Delegation).join(
            latest_dates,
            and_(
                Delegation.delegator_address == latest_dates.c.delegator_address,
                Delegation.validator_address == latest_dates.c.validator_address,
                Delegation.date == latest_dates.c.latest_date
            )
        ).all()
        
        result = {}
        for delegation in delegations:
            key = f"{delegation.delegator_address}_{delegation.validator_address}"
            result[key] = float(delegation.amount_tia)
        
        logger.info(f"📊 Found {len(result)} previous delegations for comparison")
        return result
    finally:
        session.close()

def process_deleted_delegations(previous_delegations: Dict[str, float], current_delegations: List[Dict], target_date: date) -> Tuple[int, set]:
    """
    Process delegations that were deleted (not in current list but were in previous).
    Create new records with zero amount for the current date to preserve history.
    """
    logger.info("🔍 Finding deleted delegations...")
    
    # Create set of current delegation keys
    current_keys = set()
    for delegation in current_delegations:
        key = f"{delegation['delegator_address']}_{delegation['validator_address']}"
        current_keys.add(key)
    
    # Find deleted delegations
    deleted_delegations = []
    affected_combinations = set()
    for key, amount in previous_delegations.items():
        if key not in current_keys and amount > 0:
            # This delegation was deleted
            delegator, validator = key.split('_', 1)
            deleted_delegations.append({
                'delegator_address': delegator,
                'validator_address': validator,
                'amount_tia': 0.0,
                'date': target_date
            })
            # Track this combination for flag update
            affected_combinations.add(f"{delegator}|{validator}")
    
    if not deleted_delegations:
        logger.info("✅ No deleted delegations found")
        return 0, set()
    
    # Create new records with zero amount for deleted delegations
    try:
        session = SessionLocal()
        try:
            created_count = 0
            
            for delegation_data in deleted_delegations:
                # Check if record already exists for this date
                existing = session.query(Delegation).filter(
                    Delegation.delegator_address == delegation_data['delegator_address'],
                    Delegation.validator_address == delegation_data['validator_address'],
                    Delegation.date == target_date
                ).first()
                
                if existing:
                    # Update existing record to zero
                    existing.amount_tia = 0.0
                    logger.info(f"🔄 Updated existing record to zero: {delegation_data['delegator_address']} -> {delegation_data['validator_address']}")
                else:
                    # Create new record with zero amount
                    delegation_obj = Delegation(
                        delegator_address=delegation_data['delegator_address'],
                        validator_address=delegation_data['validator_address'],
                        amount_tia=delegation_data['amount_tia'],
                        date=delegation_data['date'],
                        validator_id=None,  # Will be set later if needed
                        created_at=datetime.utcnow(),
                        is_latest=True  # New records are latest by default
                    )
                    session.add(delegation_obj)
                    created_count += 1
            
            session.commit()
            logger.info(f"✅ Processed {len(deleted_delegations)} deleted delegations: {created_count} created, {len(deleted_delegations) - created_count} updated")
            return len(deleted_delegations), affected_combinations
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Error saving deleted delegations: {e}")
        return 0, set()

def process_disappeared_delegations(current_delegations: List[Dict], target_date: date) -> int:
    """
    Process delegations that exist in DB but are not in current API response.
    These are delegations that were undelegated (set to zero).
    """
    logger.info("🔍 Finding delegations that disappeared from API...")
    
    try:
        session = SessionLocal()
        try:
            # Get all delegations from DB for the target date
            db_delegations = session.query(Delegation).filter(
                Delegation.date == target_date,
                Delegation.amount_tia > 0  # Only active delegations
            ).all()
            
            # Create set of current API delegation keys
            current_keys = set()
            for delegation in current_delegations:
                key = f"{delegation['delegator_address']}_{delegation['validator_address']}"
                current_keys.add(key)
            
            # Find delegations that exist in DB but not in current API
            disappeared_delegations = []
            for db_delegation in db_delegations:
                key = f"{db_delegation.delegator_address}_{db_delegation.validator_address}"
                if key not in current_keys:
                    # This delegation disappeared from API - set to zero
                    disappeared_delegations.append({
                        'delegator_address': db_delegation.delegator_address,
                        'validator_address': db_delegation.validator_address,
                        'amount_tia': 0.0,
                        'date': target_date
                    })
            
            if not disappeared_delegations:
                logger.info("✅ No disappeared delegations found")
                return 0
            
            # Update existing records to zero amount (they already exist for this date)
            updated_count = 0
            for delegation_data in disappeared_delegations:
                # Update existing record to zero
                result = session.query(Delegation).filter(
                    Delegation.delegator_address == delegation_data['delegator_address'],
                    Delegation.validator_address == delegation_data['validator_address'],
                    Delegation.date == target_date,
                    Delegation.amount_tia > 0  # Only update active delegations
                ).update({
                    'amount_tia': 0.0,
                })
                if result > 0:
                    updated_count += 1
                    logger.info(f"🔄 Updated disappeared delegation to zero: {delegation_data['delegator_address']} -> {delegation_data['validator_address']}")
            
            session.commit()
            logger.info(f"✅ Updated {updated_count} disappeared delegation records to zero amount")
            return updated_count
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Error processing disappeared delegations: {e}")
        return 0


def get_existing_delegations_for_date(target_date: date) -> Dict[str, float]:
    """
    Get delegations that already exist for the target_date to avoid duplicates.
    """
    logger.info(f"🔍 Checking for existing delegations on {target_date}")
    
    session = SessionLocal()
    try:
        delegations = session.query(Delegation).filter(
            Delegation.date == target_date
        ).all()
        
        result = {}
        for delegation in delegations:
            key = f"{delegation.delegator_address}_{delegation.validator_address}"
            result[key] = float(delegation.amount_tia)
        
        logger.info(f"📊 Found {len(result)} existing delegations for {target_date}")
        return result
    finally:
        session.close()

def create_delegation_record(delegation_data: Dict, target_date: date) -> Dict:
    """
    Create delegation record for saving.
    """
    # Validate data
    if not delegation_data.get('delegator_address') or not delegation_data.get('validator_address'):
        raise ValueError(f"Invalid delegation data: {delegation_data}")
    
    return {
        'delegator_address': delegation_data['delegator_address'],
        'validator_address': delegation_data['validator_address'],
        'amount_tia': delegation_data['amount_tia'],
        'date': target_date,
        'validator_id': delegation_data.get('validator_id'),
        'created_at': datetime.utcnow()
    }

def get_import_progress() -> Dict:
    """
    Return import progress information.
    """
    try:
        session = SessionLocal()
        try:
            # Last import date
            last_import = session.query(Delegation).order_by(
                desc(Delegation.date)
            ).first()
            
            if not last_import:
                return {"status": "no_data", "message": "Import has not been performed yet"}
            
            # Number of records for last date
            last_date_count = session.query(Delegation).filter(
                Delegation.date == last_import.date
            ).count()
            
            # Total number of unique delegator-validator pairs
            total_pairs = session.query(
                Delegation.delegator_address, 
                Delegation.validator_address
            ).distinct().count()
            
            return {
                "status": "success",
                "last_import_date": last_import.date.isoformat(),
                "last_import_count": last_date_count,
                "total_unique_pairs": total_pairs,
                "total_records": session.query(Delegation).count()
            }
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"Error getting progress: {e}")
        return {"status": "error", "message": str(e)}


def update_latest_delegation_flags(affected_combinations: set):
    """
    Update is_latest flags for delegation records.
    Sets is_latest=False for affected combinations that are NOT from today.
    New records already have is_latest=True by default.
    
    Args:
        affected_combinations: Set of "delegator_address|validator_address" combinations to update.
    """
    logger.info("🔄 Updating is_latest flags for delegation records...")
    
    try:
        session = SessionLocal()
        try:
            # Update only affected combinations
            logger.info(f"📊 Updating flags for {len(affected_combinations)} affected combinations...")
            
            # Set False for affected combinations that are NOT from latest date
            latest_date = session.query(func.max(Delegation.date)).scalar()
            for combination in affected_combinations:
                delegator, validator = combination.split('|')
                session.query(Delegation).filter(
                    and_(
                        Delegation.delegator_address == delegator,
                        Delegation.validator_address == validator,
                        Delegation.date != latest_date
                    )
                ).update({'is_latest': False})
            
            
            logger.info(f"✅ Set is_latest=False for non-latest records in {len(affected_combinations)} combinations")
            
            session.commit()
            logger.info(f"✅ Updated is_latest flags for {len(affected_combinations)} combinations")
                
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Error updating is_latest flags: {e}")
        raise


def update_validator_delegation_stats():
    """
    Update validator table with aggregated delegation statistics.
    """
    logger.info("🔄 Updating validator delegation statistics...")
    
    try:
        session = SessionLocal()
        try:
            # Get all validators
            validators = session.query(Validator).all()
            logger.info(f"📋 Processing {len(validators)} validators...")
            
            updated_count = 0
            for validator in validators:
                # Get delegation statistics for this validator
                delegation_stats = session.query(
                    func.count(Delegation.id).label('delegation_count'),
                    func.count(func.distinct(Delegation.delegator_address)).label('total_delegators'),
                    func.sum(Delegation.amount_tia).label('total_delegations')
                ).filter(
                    Delegation.validator_address == validator.operator_address
                ).first()
                
                # Update validator with delegation statistics
                validator.total_delegations = float(delegation_stats.total_delegations or 0)
                validator.total_delegators = delegation_stats.total_delegators or 0
                
                # Update the validator record
                session.add(validator)
                updated_count += 1
                
                if updated_count % 50 == 0:
                    logger.info(f"📊 Updated {updated_count}/{len(validators)} validators...")
            
            # Commit all changes
            session.commit()
            logger.info(f"✅ Successfully updated delegation statistics for {updated_count} validators")
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Error updating validator delegation statistics: {e}")
        raise
