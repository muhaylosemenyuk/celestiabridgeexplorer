#!/usr/bin/env python3
"""
Delegation Data Export Service
Exports delegation data from database in various formats
"""

import json
import csv
import logging
import sys
import os
from typing import Dict, Optional, Any
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.universal_db_aggregator import aggregate_db_data
from models.delegation import Delegation

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DelegationExporter:
    """Class for exporting delegation data"""
    
    def __init__(self):
        logger.info("Initializing DelegationExporter")
    
    def export_to_json(self, output_file: str = None, filters: Optional[Dict[str, Any]] = None) -> str:
        """Export delegations to JSON format"""
        logger.info("Exporting delegations to JSON...")
        
        # Get data from database
        result = aggregate_db_data(
            model_class=Delegation,
            filters=filters,
            return_format="list"
        )
        
        delegations = result.get("results", [])
        logger.info(f"Exported {len(delegations)} delegations")
        
        # Form output file
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"delegations_export_{timestamp}.json"
        
        # Add metadata
        export_data = {
            "export_info": {
                "timestamp": datetime.now().isoformat(),
                "total_delegations": len(delegations),
                "filters_applied": filters or {}
            },
            "delegations": delegations
        }
        
        # Write file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"JSON export completed: {output_file}")
        return output_file
    
    def export_to_csv(self, output_file: str = None, filters: Optional[Dict[str, Any]] = None) -> str:
        """Export delegations to CSV format"""
        logger.info("Exporting delegations to CSV...")
        
        # Get data from database
        result = aggregate_db_data(
            model_class=Delegation,
            filters=filters,
            return_format="list"
        )
        
        delegations = result.get("results", [])
        logger.info(f"Exported {len(delegations)} delegations")
        
        if not delegations:
            logger.warning("No data to export")
            return None
        
        # Form output file
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"delegations_export_{timestamp}.csv"
        
        # Define headers
        headers = delegations[0].keys()
        
        # Write CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(delegations)
        
        logger.info(f"CSV export completed: {output_file}")
        return output_file
    
    def get_delegation_statistics(self) -> Dict[str, Any]:
        """Get delegation statistics"""
        logger.info("Getting delegation statistics...")
        
        # General statistics
        total_stats = aggregate_db_data(
            model_class=Delegation,
            aggregations=[
                {"type": "count"},
                {"type": "sum", "field": "amount_tia"},
                {"type": "avg", "field": "amount_tia"},
                {"type": "max", "field": "amount_tia"},
                {"type": "min", "field": "amount_tia"}
            ],
            return_format="aggregated"
        )
        
        # Statistics by date
        date_stats = aggregate_db_data(
            model_class=Delegation,
            group_by=["date"],
            aggregations=[
                {"type": "count"},
                {"type": "sum", "field": "amount_tia"}
            ],
            order_by={"date": "desc"},
            limit=30,  # Last 30 days
            return_format="list"
        )
        
        # Top delegators by total amount
        top_delegators = aggregate_db_data(
            model_class=Delegation,
            group_by=["delegator_address"],
            aggregations=[
                {"type": "count"},
                {"type": "sum", "field": "amount_tia"}
            ],
            order_by={"sum_amount_tia": "desc"},
            limit=20,
            return_format="list"
        )
        
        # Top validators by total delegations
        top_validators = aggregate_db_data(
            model_class=Delegation,
            group_by=["validator_address"],
            aggregations=[
                {"type": "count"},
                {"type": "sum", "field": "amount_tia"}
            ],
            order_by={"sum_amount_tia": "desc"},
            limit=20,
            return_format="list"
        )
        
        # Delegation distribution by amount ranges
        amount_ranges = [
            {"min": 0, "max": 1000, "label": "0-1K TIA"},
            {"min": 1000, "max": 10000, "label": "1K-10K TIA"},
            {"min": 10000, "max": 100000, "label": "10K-100K TIA"},
            {"min": 100000, "max": 1000000, "label": "100K-1M TIA"},
            {"min": 1000000, "max": None, "label": "1M+ TIA"}
        ]
        
        range_stats = []
        for range_info in amount_ranges:
            filters = {"amount_tia": {"gte": range_info["min"]}}
            if range_info["max"] is not None:
                filters["amount_tia"]["lte"] = range_info["max"]
            
            range_result = aggregate_db_data(
                model_class=Delegation,
                filters=filters,
                aggregations=[
                    {"type": "count"},
                    {"type": "sum", "field": "amount_tia"}
                ],
                return_format="aggregated"
            )
            
            range_stats.append({
                "range": range_info["label"],
                "count": range_result.get("count", 0),
                "total_amount": range_result.get("sum_amount_tia", 0)
            })
        
        return {
            "total_statistics": total_stats,
            "date_statistics": date_stats.get("results", []),
            "top_delegators": top_delegators.get("results", []),
            "top_validators": top_validators.get("results", []),
            "amount_range_distribution": range_stats,
            "generated_at": datetime.now().isoformat()
        }
    
    def export_statistics(self, output_file: str = None) -> str:
        """Export statistics to JSON"""
        logger.info("Exporting delegation statistics...")
        
        stats = self.get_delegation_statistics()
        
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"delegations_statistics_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Statistics exported: {output_file}")
        return output_file
    
    def get_delegator_delegations(self, delegator_address: str, limit: int = 100) -> Dict[str, Any]:
        """Get all delegations for a specific delegator"""
        logger.info(f"Getting delegations for delegator: {delegator_address}")
        
        filters = {"delegator_address": {"eq": delegator_address}}
        
        result = aggregate_db_data(
            model_class=Delegation,
            filters=filters,
            order_by={"amount_tia": "desc"},
            limit=limit,
            return_format="list"
        )
        
        delegations = result.get("results", [])
        
        # Calculate total delegated amount
        total_delegated = sum(float(d.get("amount_tia", 0)) for d in delegations)
        
        return {
            "delegator_address": delegator_address,
            "total_delegations": len(delegations),
            "total_delegated_amount": total_delegated,
            "delegations": delegations,
            "generated_at": datetime.now().isoformat()
        }
    
    def get_validator_delegations(self, validator_address: str, limit: int = 100) -> Dict[str, Any]:
        """Get all delegations for a specific validator"""
        logger.info(f"Getting delegations for validator: {validator_address}")
        
        filters = {"validator_address": {"eq": validator_address}}
        
        result = aggregate_db_data(
            model_class=Delegation,
            filters=filters,
            order_by={"amount_tia": "desc"},
            limit=limit,
            return_format="list"
        )
        
        delegations = result.get("results", [])
        
        # Calculate total delegated amount
        total_delegated = sum(float(d.get("amount_tia", 0)) for d in delegations)
        
        return {
            "validator_address": validator_address,
            "total_delegations": len(delegations),
            "total_delegated_amount": total_delegated,
            "delegations": delegations,
            "generated_at": datetime.now().isoformat()
        }
    
    def get_delegation_history(self, delegator_address: str, validator_address: str) -> Dict[str, Any]:
        """Get delegation history for a specific delegator-validator pair"""
        logger.info(f"Getting delegation history for {delegator_address} -> {validator_address}")
        
        filters = {
            "delegator_address": {"eq": delegator_address},
            "validator_address": {"eq": validator_address}
        }
        
        result = aggregate_db_data(
            model_class=Delegation,
            filters=filters,
            order_by={"date": "asc"},
            return_format="list"
        )
        
        delegations = result.get("results", [])
        
        return {
            "delegator_address": delegator_address,
            "validator_address": validator_address,
            "total_records": len(delegations),
            "history": delegations,
            "generated_at": datetime.now().isoformat()
        }

