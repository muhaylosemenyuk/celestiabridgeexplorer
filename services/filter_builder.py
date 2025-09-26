#!/usr/bin/env python3
"""
Universal Filter Builder
Provides unified filter handling for all API endpoints
"""

import logging
from typing import Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class FilterBuilder:
    """Universal filter builder for API endpoints"""
    
    def __init__(self, model_class):
        self.model_class = model_class
        self.filters = {}
        logger.debug(f"Initialized FilterBuilder for {model_class.__name__}")
    
    def build_from_params(self, params: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build filters from request parameters using configuration
        
        Args:
            params: Request parameters
            config: Filter configuration
            
        Returns:
            Dict of filters ready for universal_db_aggregator
        """
        logger.debug(f"Building filters from params: {list(params.keys())}")
        
        # Process exact matches
        if 'exact' in config:
            for field in config['exact']:
                if field in params and params[field] is not None:
                    self.add_exact(field, params[field])
        
        # Process like matches (partial text search)
        if 'like' in config:
            for field in config['like']:
                if field in params and params[field] is not None:
                    self.add_like(field, params[field])
        
        # Process range filters
        if 'range' in config:
            for field_config in config['range']:
                if isinstance(field_config, tuple) and len(field_config) == 3:
                    field, min_param, max_param = field_config
                    min_val = params.get(min_param)
                    max_val = params.get(max_param)
                    if min_val is not None or max_val is not None:
                        self.add_range(field, min_val, max_val)
        
        # Process date range filters
        if 'date_range' in config:
            for field_config in config['date_range']:
                if isinstance(field_config, tuple) and len(field_config) == 3:
                    field, after_param, before_param = field_config
                    after_val = params.get(after_param)
                    before_val = params.get(before_param)
                    if after_val is not None or before_val is not None:
                        self.add_date_range(field, after_val, before_val)
        
        # Process custom filters
        if 'custom' in config:
            for custom_filter in config['custom']:
                if callable(custom_filter):
                    custom_filter(self, params)
        
        logger.debug(f"Built filters: {self.filters}")
        return self.filters
    
    def add_exact(self, field: str, value: Any) -> 'FilterBuilder':
        """Add exact match filter"""
        if value is not None:
            self.filters[field] = value
            logger.debug(f"Added exact filter: {field} = {value}")
        return self
    
    def add_like(self, field: str, value: str) -> 'FilterBuilder':
        """Add partial text search filter"""
        if value is not None and value.strip():
            self.filters[field] = {"like": value}
            logger.debug(f"Added like filter: {field} LIKE {value}")
        return self
    
    def add_range(self, field: str, min_val: Optional[Union[int, float]], max_val: Optional[Union[int, float]]) -> 'FilterBuilder':
        """Add range filter (min/max values)"""
        if min_val is not None or max_val is not None:
            if field not in self.filters:
                self.filters[field] = {}
            
            if min_val is not None:
                self.filters[field]["gte"] = min_val
                logger.debug(f"Added range filter: {field} >= {min_val}")
            
            if max_val is not None:
                self.filters[field]["lte"] = max_val
                logger.debug(f"Added range filter: {field} <= {max_val}")
        return self
    
    def add_date_range(self, field: str, after: Optional[str], before: Optional[str]) -> 'FilterBuilder':
        """Add date range filter"""
        if after is not None or before is not None:
            if field not in self.filters:
                self.filters[field] = {}
            
            if after is not None:
                self.filters[field]["gte"] = after
                logger.debug(f"Added date range filter: {field} >= {after}")
            
            if before is not None:
                self.filters[field]["lte"] = before
                logger.debug(f"Added date range filter: {field} <= {before}")
        return self
    
    def add_custom(self, field: str, filter_dict: Dict[str, Any]) -> 'FilterBuilder':
        """Add custom filter dictionary"""
        if filter_dict:
            # If field already exists, merge the filters
            if field in self.filters and isinstance(self.filters[field], dict):
                self.filters[field].update(filter_dict)
                logger.debug(f"Merged custom filter: {field} = {self.filters[field]}")
            else:
                self.filters[field] = filter_dict
                logger.debug(f"Added custom filter: {field} = {filter_dict}")
        return self
    
    def get_filters(self) -> Dict[str, Any]:
        """Get built filters"""
        return self.filters
    
    def clear(self) -> 'FilterBuilder':
        """Clear all filters"""
        self.filters = {}
        logger.debug("Cleared all filters")
        return self


def build_filters(model_class, params: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience function to build filters
    
    Args:
        model_class: SQLAlchemy model class
        params: Request parameters
        config: Filter configuration
        
    Returns:
        Dict of filters ready for universal_db_aggregator
    """
    builder = FilterBuilder(model_class)
    return builder.build_from_params(params, config)
