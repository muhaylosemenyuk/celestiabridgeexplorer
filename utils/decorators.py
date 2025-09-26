#!/usr/bin/env python3
"""
API Decorators
Provides decorators for automatic filter handling
"""

import logging
from functools import wraps
from typing import Callable
from services.filter_builder import FilterBuilder
from filter_configs.filter_configs import get_filter_config, validate_field, get_valid_fields

logger = logging.getLogger(__name__)

def with_filters(config_name: str):
    """
    Decorator for automatic filter handling
    
    Args:
        config_name: Name of filter configuration to use
        
    Returns:
        Decorated function with automatic filter processing
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get filter configuration
            config = get_filter_config(config_name)
            if not config:
                logger.warning(f"No filter configuration found for {config_name}")
                return func(*args, **kwargs)
            
            # Extract model class from function signature or kwargs
            model_class = kwargs.get('model_class')
            if not model_class:
                logger.error(f"No model_class provided for {config_name}")
                return func(*args, **kwargs)
            
            # Build filters from parameters
            filter_builder = FilterBuilder(model_class)
            filters = filter_builder.build_from_params(kwargs, config)
            
            # Add filters to kwargs
            kwargs['filters'] = filters if filters else None
            
            logger.debug(f"Applied filters for {config_name}: {filters}")
            
            # Call original function with processed filters
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

def validate_endpoint_fields(endpoint_name: str):
    """
    Decorator for field validation
    
    Args:
        endpoint_name: Name of endpoint for field validation
        
    Returns:
        Decorated function with field validation
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Validate order_by field
            order_by = kwargs.get('order_by')
            if order_by and not validate_field(endpoint_name, order_by):
                return {
                    "error": f"Invalid field '{order_by}' for sorting. Valid fields: {get_valid_fields(endpoint_name)}"
                }
            
            # Validate group_by fields
            group_by = kwargs.get('group_by')
            if group_by:
                group_by_list = [field.strip() for field in group_by.split(",")]
                for field in group_by_list:
                    if not validate_field(endpoint_name, field):
                        return {
                            "error": f"Invalid field '{field}' for grouping. Valid fields: {get_valid_fields(endpoint_name)}"
                        }
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

def with_validation_and_filters(config_name: str):
    """
    Combined decorator for filters and validation
    
    Args:
        config_name: Name of filter configuration to use
        
    Returns:
        Decorated function with both filter processing and validation
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Apply validation first
            validation_decorator = validate_endpoint_fields(config_name)
            validated_func = validation_decorator(func)
            
            # Apply filters
            filter_decorator = with_filters(config_name)
            return filter_decorator(validated_func)(*args, **kwargs)
        
        return wrapper
    return decorator
