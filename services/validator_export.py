#!/usr/bin/env python3
"""
Validator Data Export Service
Exports data from database in various formats
"""

import json
import csv
import logging
import sys
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.db import SessionLocal
from services.universal_db_aggregator import aggregate_db_data
from models.validator import Validator

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ValidatorExporter:
    """Class for exporting validator data"""
    
    def __init__(self):
        logger.info("Initializing ValidatorExporter")
    
    def export_to_json(self, output_file: str = None, filters: Optional[Dict[str, Any]] = None) -> str:
        """Export validators to JSON format"""
        logger.info("Exporting validators to JSON...")
        
        # Get data from database
        result = aggregate_db_data(
            model_class=Validator,
            filters=filters,
            return_format="list"
        )
        
        validators = result.get("results", [])
        logger.info(f"Exported {len(validators)} validators")
        
        # Form output file
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"validators_export_{timestamp}.json"
        
        # Add metadata
        export_data = {
            "export_info": {
                "timestamp": datetime.now().isoformat(),
                "total_validators": len(validators),
                "filters_applied": filters or {}
            },
            "validators": validators
        }
        
        # Write file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"JSON export completed: {output_file}")
        return output_file
    
    def export_to_csv(self, output_file: str = None, filters: Optional[Dict[str, Any]] = None) -> str:
        """Export validators to CSV format"""
        logger.info("Exporting validators to CSV...")
        
        # Get data from database
        result = aggregate_db_data(
            model_class=Validator,
            filters=filters,
            return_format="list"
        )
        
        validators = result.get("results", [])
        logger.info(f"Exported {len(validators)} validators")
        
        if not validators:
            logger.warning("No data to export")
            return None
        
        # Form output file
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"validators_export_{timestamp}.csv"
        
        # Define headers
        headers = validators[0].keys()
        
        # Write CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(validators)
        
        logger.info(f"CSV export completed: {output_file}")
        return output_file
    
    def get_validator_statistics(self) -> Dict[str, Any]:
        """Get validator statistics"""
        logger.info("Getting validator statistics...")
        
        # General statistics
        total_stats = aggregate_db_data(
            model_class=Validator,
            aggregations=[
                {"type": "count"},
                {"type": "sum", "field": "tokens"},
                {"type": "avg", "field": "commission_rate"},
                {"type": "avg", "field": "uptime_percent"},
                {"type": "sum", "field": "total_delegators"}
            ],
            return_format="aggregated"
        )
        
        # Statistics by status
        status_stats = aggregate_db_data(
            model_class=Validator,
            group_by=["status"],
            aggregations=[{"type": "count"}],
            order_by={"count": "desc"}
        )
        
        # Top validators by voting power
        top_voting_power = aggregate_db_data(
            model_class=Validator,
            order_by={"voting_power": "desc"},
            limit=10
        )
        
        # Top validators by uptime
        top_uptime = aggregate_db_data(
            model_class=Validator,
            filters={"uptime_percent": {"is_null": False}},
            order_by={"uptime_percent": "desc"},
            limit=10
        )
        
        # Commission statistics
        commission_stats = aggregate_db_data(
            model_class=Validator,
            filters={"commission_rate": {"is_null": False}},
            aggregations=[
                {"type": "min", "field": "commission_rate"},
                {"type": "max", "field": "commission_rate"},
                {"type": "avg", "field": "commission_rate"}
            ],
            return_format="aggregated"
        )
        
        return {
            "total_statistics": total_stats,
            "status_distribution": status_stats,
            "top_voting_power": top_voting_power,
            "top_uptime": top_uptime,
            "commission_statistics": commission_stats,
            "generated_at": datetime.now().isoformat()
        }
    
    def export_statistics(self, output_file: str = None) -> str:
        """Export statistics to JSON"""
        logger.info("Exporting validator statistics...")
        
        stats = self.get_validator_statistics()
        
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"validators_statistics_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Statistics exported: {output_file}")
        return output_file
    
    

