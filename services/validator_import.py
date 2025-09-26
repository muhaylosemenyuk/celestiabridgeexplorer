#!/usr/bin/env python3
"""
Celestia Validator Data Import Service
Collects validator information from various API endpoints
"""

import json
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import existing components
from services.cosmos_api import make_cosmos_request
from services.paginated_aggregator import fetch_and_aggregate_paginated
from services.db import SessionLocal
from models.validator import Validator
import config

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ValidatorImporter:
    """Class for importing validator data"""
    
    def __init__(self):
        logger.info("Initializing ValidatorImporter")
    
    def get_slashing_params(self) -> Optional[Dict[str, Any]]:
        """Get slashing parameters (including signed_blocks_window)"""
        logger.info("Getting slashing parameters...")
        data = make_cosmos_request("/cosmos/slashing/v1beta1/params")
        if data:
            logger.info(f"Slashing parameters: {data}")
            return data
        return None
    
    def get_validators_paginated(self, status: str = None) -> List[Dict[str, Any]]:
        """Get all validators with pagination using paginated_aggregator"""
        if status:
            logger.info(f"Getting validators with status {status}...")
            params = {"status": status}
        else:
            logger.info("Getting all validators (all statuses)...")
            params = {}
        
        # Use direct API call with high limit for getting validators
        endpoint = "/cosmos/staking/v1beta1/validators"
        params = {"pagination.limit": 100000}  # Same limit as original get_validators
        if status:
            params["status"] = status
        
        validators_data = make_cosmos_request(endpoint, params)
        if not validators_data or 'validators' not in validators_data:
            return []
        
        validators = validators_data['validators']
        
        logger.info(f"Total received {len(validators)} validators")
        return validators
    
    def get_validator_signing_info(self, validator_consensus_address: str) -> Optional[Dict[str, Any]]:
        """Get block signing information for validator"""
        endpoint = f"/cosmos/slashing/v1beta1/signing_infos/{validator_consensus_address}"
        return make_cosmos_request(endpoint)
    
    def get_latest_validator_set(self) -> List[Dict[str, Any]]:
        """Get latest validator set from Tendermint"""
        logger.info("Getting latest validator set...")
        
        # Try different endpoints for validator set
        endpoints_to_try = [
            "/cosmos/base/tendermint/v1beta1/validatorsets/latest",
            "/cosmos/base/tendermint/v1beta1/validatorsets/height/latest"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                validators = fetch_and_aggregate_paginated(
                    endpoint=endpoint,
                    params={},
                    item_path="validators",
                    aggregate="all"
                )
                if validators:
                    logger.info(f"Received {len(validators)} validators from validator set via {endpoint}")
                    return validators
            except Exception as e:
                logger.warning(f"Failed to get validator set via {endpoint}: {e}")
                continue
        
        # If failed via paginated_aggregator, try directly
        for endpoint in endpoints_to_try:
            data = make_cosmos_request(endpoint)
            if data:
                # Different possible response structures
                validators = []
                if "validators" in data:
                    validators = data["validators"]
                elif "validator_set" in data and "validators" in data["validator_set"]:
                    validators = data["validator_set"]["validators"]
                elif "result" in data and "validators" in data["result"]:
                    validators = data["result"]["validators"]
                
                if validators:
                    logger.info(f"Received {len(validators)} validators from validator set via {endpoint}")
                    return validators
        
        logger.warning("Failed to get validator set")
        return []
    
    def analyze_validator_data(self, validators: List[Dict[str, Any]]) -> None:
        """Analyze validator data structure"""
        logger.info("=== VALIDATOR DATA STRUCTURE ANALYSIS ===")
        
        if not validators:
            logger.warning("No data to analyze")
            return
        
        # Analyze first validator
        first_validator = validators[0]
        logger.info(f"Example validator structure:")
        logger.info(json.dumps(first_validator, indent=2, ensure_ascii=False))
        
        # Analyze all keys
        all_keys = set()
        for validator in validators:
            all_keys.update(validator.keys())
        
        logger.info(f"All available keys: {sorted(all_keys)}")
        
        # Analyze specific fields
        key_analysis = {}
        for key in all_keys:
            values = []
            for validator in validators[:10]:  # Analyze first 10
                if key in validator:
                    values.append(validator[key])
            key_analysis[key] = values
        
        logger.info("=== FIELD ANALYSIS ===")
        for key, values in key_analysis.items():
            logger.info(f"{key}: {values}")
    
    
    
    def collect_complete_validator_data(self, validator_address: str, validator_set_dict: Dict[str, Any] = None) -> Dict[str, Any]:
        """Collect complete validator information (without delegations)"""
        logger.info(f"Collecting complete information for validator {validator_address}...")
        
        # Basic information from Cosmos API
        validator_info = make_cosmos_request(f"/cosmos/staking/v1beta1/validators/{validator_address}")
        
        # Signing info (needs consensus address, not pubkey)
        signing_info = None
        if validator_info and validator_set_dict:
            validator_data = validator_info.get("validator", {})
            consensus_pubkey = validator_data.get("consensus_pubkey", {}).get("key")
            if consensus_pubkey and consensus_pubkey in validator_set_dict:
                consensus_address = validator_set_dict[consensus_pubkey].get("address")
                if consensus_address:
                    signing_info = self.get_validator_signing_info(consensus_address)
        
        return {
            "validator": validator_info.get("validator") if validator_info else None,
            "signing_info": signing_info
        }
    
    def normalize_validator_data(self, validator_data: Dict[str, Any], validator_set_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Normalize validator data for database storage"""
        # Get basic validator information
        validator_info = validator_data.get("validator", {})
        
        # Convert utia to TIA for tokens and min_self_delegation
        tokens_utia = validator_info.get("tokens")
        min_self_delegation_utia = validator_info.get("min_self_delegation")
        
        # Basic fields
        normalized = {
            "operator_address": validator_info.get("operator_address"),
            "consensus_pubkey": validator_info.get("consensus_pubkey", {}).get("key"),
            "jailed": validator_info.get("jailed", False),
            "status": validator_info.get("status"),
            "tokens": self.convert_utia_to_tia(tokens_utia) if tokens_utia else None,
            "min_self_delegation": self.convert_utia_to_tia(min_self_delegation_utia) if min_self_delegation_utia else None
        }
        
        # Description
        description = validator_info.get("description", {})
        normalized.update({
            "moniker": description.get("moniker"),
            "identity": description.get("identity"),
            "website": description.get("website"),
            "security_contact": description.get("security_contact"),
            "details": description.get("details")
        })
        
        # Commission
        commission = validator_info.get("commission", {})
        commission_rates = commission.get("commission_rates", {})
        
        # Parse commission date
        commission_update_time = None
        if commission.get("update_time"):
            try:
                from datetime import datetime
                commission_update_time = datetime.fromisoformat(commission["update_time"].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                commission_update_time = None
        
        normalized.update({
            "commission_rate": commission_rates.get("rate"),
            "max_commission_rate": commission_rates.get("max_rate"),
            "max_change_rate": commission_rates.get("max_change_rate"),
            "commission_update_time": commission_update_time
        })
        
        # Data from validator set
        if validator_set_data:
            normalized.update({
                "consensus_address": validator_set_data.get("address"),
                "voting_power": validator_set_data.get("voting_power"),
                "proposer_priority": validator_set_data.get("proposer_priority")
            })
        
        # Signing info
        signing_info = validator_data.get("signing_info", {})
        if signing_info and "val_signing_info" in signing_info:
            val_signing_info = signing_info["val_signing_info"]
            normalized.update({
                "missed_blocks_counter": val_signing_info.get("missed_blocks_counter", 0)
            })
        
        # Calculate uptime
        if normalized.get("missed_blocks_counter") is not None:
            slashing_params = validator_data.get("slashing_params", {})
            window = slashing_params.get("params", {}).get("signed_blocks_window", 0)
            if window and int(window) > 0:
                missed = int(normalized["missed_blocks_counter"])
                uptime = (1 - missed / int(window)) * 100
                normalized["uptime_percent"] = round(uptime, 2)
        
        # Delegation statistics - preserve existing values if they exist
        # Only set to 0 if this is a new validator
        if "total_delegations" not in normalized:
            normalized["total_delegations"] = 0
        if "total_delegators" not in normalized:
            normalized["total_delegators"] = 0
        
        return normalized
    
    def convert_utia_to_tia(self, utia_amount):
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
    
    def save_validator_to_db(self, validator_data: Dict[str, Any]) -> bool:
        """Save validator to database"""
        try:
            # Check for required fields
            if not validator_data.get("operator_address"):
                logger.error("operator_address is required but not provided")
                return False
            
            session = SessionLocal()
            
            # Check if validator exists
            existing = session.query(Validator).filter(
                Validator.operator_address == validator_data["operator_address"]
            ).first()
            
            if existing:
                # Update existing - preserve delegation statistics
                delegation_stats = {
                    'total_delegations': existing.total_delegations,
                    'total_delegators': existing.total_delegators
                }
                
                for key, value in validator_data.items():
                    if hasattr(existing, key) and value is not None:
                        # Skip delegation statistics - preserve existing values
                        if key not in ['total_delegations', 'total_delegators']:
                            setattr(existing, key, value)
                
                # Restore delegation statistics
                existing.total_delegations = delegation_stats['total_delegations']
                existing.total_delegators = delegation_stats['total_delegators']
                
                session.commit()
                logger.info(f"Updated validator: {validator_data['operator_address']} (preserved delegation stats)")
            else:
                # Create new
                validator = Validator(**validator_data)
                session.add(validator)
                session.commit()
                logger.info(f"Created new validator: {validator_data['operator_address']}")
            
            session.close()
            return True
            
        except Exception as e:
            logger.error(f"Error saving validator {validator_data.get('operator_address', 'unknown')}: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return False
    
    def import_all_validators(self) -> Dict[str, Any]:
        """Import all validators with parallel processing"""
        return self.import_all_validators_parallel()
    
    def process_single_validator(self, validator: Dict[str, Any], validator_set_dict: Dict[str, Any], slashing_params: Dict[str, Any], index: int, total: int) -> Dict[str, Any]:
        """Process single validator"""
        validator_address = validator.get('operator_address', 'unknown')
        logger.info(f"Processing validator {index}/{total}: {validator_address}")
        
        try:
            # Get additional information
            complete_data = self.collect_complete_validator_data(validator_address, validator_set_dict)
            
            # Add slashing parameters
            complete_data["slashing_params"] = slashing_params
            
            # Find corresponding data from validator set
            consensus_pubkey = validator.get("consensus_pubkey", {}).get("key")
            validator_set_data = validator_set_dict.get(consensus_pubkey) if consensus_pubkey else None
            
            # Normalize data
            normalized_data = self.normalize_validator_data(complete_data, validator_set_data)
            
            # Save to database
            if self.save_validator_to_db(normalized_data):
                return {"success": True, "address": validator_address}
            else:
                return {"success": False, "address": validator_address, "error": "Save error"}
                
        except Exception as e:
            logger.error(f"Error processing validator {validator_address}: {e}")
            return {"success": False, "address": validator_address, "error": str(e)}

    def import_all_validators_parallel(self) -> Dict[str, Any]:
        """Import all validators with parallel processing"""
        logger.info("=== STARTING PARALLEL VALIDATOR IMPORT ===")
        
        import_stats = {
            "started_at": None,
            "completed_at": None,
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "errors": []
        }
        
        try:
            import_stats["started_at"] = datetime.now().isoformat()
            
            # Get slashing parameters
            slashing_params = self.get_slashing_params()
            
            # Get validators from staking module (all statuses)
            validators = self.get_validators_paginated(status=None)
            logger.info(f"Received {len(validators)} validators for import")
            
            # Get validator set for additional information
            validator_set = self.get_latest_validator_set()
            validator_set_dict = {}
            for vs in validator_set:
                pubkey = vs.get("pub_key", {}).get("key")
                if pubkey:
                    validator_set_dict[pubkey] = vs
            
            # Parallel validator processing
            batch_size = config.WALLET_BATCH_SIZE
            max_workers = min(batch_size, len(validators))  # No more than number of validators
            
            logger.info(f"Starting parallel processing with {max_workers} threads, batch size: {batch_size}")
            
            # Process validators in batches
            for i in range(0, len(validators), batch_size):
                batch = validators[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(validators) + batch_size - 1)//batch_size} ({len(batch)} validators)")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Create tasks for each validator in batch
                    future_to_validator = {
                        executor.submit(
                            self.process_single_validator, 
                            validator, 
                            validator_set_dict,
                            slashing_params,
                            i + j + 1, 
                            len(validators)
                        ): validator for j, validator in enumerate(batch)
                    }
                    
                    # Process results as they complete
                    for future in as_completed(future_to_validator):
                        result = future.result()
                        import_stats["total_processed"] += 1
                        
                        if result["success"]:
                            import_stats["successful"] += 1
                        else:
                            import_stats["failed"] += 1
                            import_stats["errors"].append(f"Error {result['address']}: {result.get('error', 'Unknown error')}")
            
            import_stats["completed_at"] = datetime.now().isoformat()
            logger.info(f"=== PARALLEL IMPORT COMPLETED ===")
            logger.info(f"Processed: {import_stats['total_processed']}")
            logger.info(f"Successful: {import_stats['successful']}")
            logger.info(f"Failed: {import_stats['failed']}")
            
        except Exception as e:
            logger.error(f"Critical parallel import error: {e}")
            import_stats["errors"].append(f"Critical error: {str(e)}")
        
        return import_stats
    
