#!/usr/bin/env python3
"""
Endpoint Helpers
Utility functions for simplified API endpoint creation
"""

import json
import logging
from typing import Dict, Any, Optional, List, Type
from fastapi import Query
from services.universal_db_aggregator import aggregate_db_data
from services.filter_builder import build_filters
from filter_configs.filter_configs import get_filter_config, get_valid_fields, validate_field

logger = logging.getLogger(__name__)

def create_standard_endpoint(
    model_class: Type,
    endpoint_name: str,
    default_order_by: str = None,
    default_order_direction: str = "desc"
):
    """
    Create a standard endpoint with automatic filter handling
    
    Args:
        model_class: SQLAlchemy model class
        endpoint_name: Name of endpoint for configuration
        default_order_by: Default field for sorting
        default_order_direction: Default sort direction
        
    Returns:
        Function that can be used as FastAPI endpoint
    """
    def endpoint(
        # Pagination
        skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"), 
        limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
        
        # Grouping
        group_by: Optional[str] = Query(None, description="Comma-separated fields to group by"),
        
        # Aggregations
        aggregations: Optional[str] = Query(None, description="JSON string with aggregations"),
        
        # Sorting
        order_by: Optional[str] = Query(default_order_by, description="Field to sort by"),
        order_direction: Optional[str] = Query(default_order_direction, description="Sort direction (asc, desc)"),
        
        # Return format
        return_format: str = Query("list", description="Return format: list, aggregated, count_only"),
        
        # Filters - will be automatically processed
        **filter_params
    ):
        """
        Standard endpoint with automatic filter processing
        """
        try:
            # Build filters using configuration
            config = get_filter_config(endpoint_name)
            filters = build_filters(model_class, filter_params, config)
            
            # Parse grouping
            group_by_list = None
            if group_by:
                group_by_list = [field.strip() for field in group_by.split(",")]
                # Validate grouping fields
                valid_fields = get_valid_fields(endpoint_name)
                for field in group_by_list:
                    if field not in valid_fields:
                        return {"error": f"Invalid field '{field}' for grouping. Valid fields: {valid_fields}"}
            
            # Parse aggregations
            parsed_aggregations = None
            if aggregations:
                try:
                    parsed_aggregations = json.loads(aggregations)
                except json.JSONDecodeError:
                    return {"error": "Invalid JSON in aggregations parameter"}
            
            # Build sorting
            order_by_dict = None
            if order_by:
                valid_fields = get_valid_fields(endpoint_name)
                if order_by not in valid_fields:
                    return {"error": f"Invalid field '{order_by}' for sorting. Valid fields: {valid_fields}"}
                order_by_dict = {order_by: order_direction}
            
            # Use universal aggregator
            return aggregate_db_data(
                model_class=model_class,
                filters=filters if filters else None,
                group_by=group_by_list,
                aggregations=parsed_aggregations,
                order_by=order_by_dict,
                limit=limit,
                offset=skip,
                return_format=return_format
            )
            
        except Exception as e:
            logger.error(f"Error in {endpoint_name} endpoint: {e}")
            return {"error": str(e)}
    
    return endpoint

def create_simple_endpoint(
    model_class: Type,
    endpoint_name: str,
    description: str = None
):
    """
    Create a simple endpoint with minimal configuration
    
    Args:
        model_class: SQLAlchemy model class
        endpoint_name: Name of endpoint for configuration
        description: Endpoint description
        
    Returns:
        Function that can be used as FastAPI endpoint
    """
    def endpoint(
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000)
    ):
        """
        Simple endpoint with basic pagination
        """
        try:
            return aggregate_db_data(
                model_class=model_class,
                limit=limit,
                offset=skip,
                return_format="list"
            )
        except Exception as e:
            logger.error(f"Error in {endpoint_name} endpoint: {e}")
            return {"error": str(e)}
    
    # Set description if provided
    if description:
        endpoint.__doc__ = description
    
    return endpoint
