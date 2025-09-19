"""
Universal aggregator for database tables.

Simple and efficient aggregator that works with any SQLAlchemy models.
Inspired by paginated_aggregator.py architecture - one main function with clear parameters.

Supports:
- Filtering by any fields with different operators
- Grouping by fields
- Aggregation (count, sum, avg, min, max)
- Sorting
- Pagination
- Working with any models
"""
import logging
from typing import Any, Dict, List, Optional, Type
from datetime import date, datetime
from sqlalchemy import func, desc, asc
from sqlalchemy.inspection import inspect

from services.db import SessionLocal

logger = logging.getLogger(__name__)


def aggregate_db_data(
    model_class: Type,
    filters: Optional[Dict[str, Any]] = None,
    group_by: Optional[List[str]] = None,
    aggregations: Optional[List[Dict[str, str]]] = None,
    order_by: Optional[Dict[str, str]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    return_format: str = "list"
) -> Dict[str, Any]:
    """
    Universal function for database data aggregation.
    
    Args:
        model_class: SQLAlchemy model class
        filters: Filters in format {field: value} or {field: {operator: value}}
        group_by: List of fields for grouping
        aggregations: List of aggregations [{"type": "count"}, {"type": "sum", "field": "field_name"}]
        order_by: Sorting {field: "asc"/"desc"}
        limit: Maximum number of results
        offset: Offset for pagination
        return_format: Return format - "list", "aggregated", "count_only"
        
    Returns:
        Dict with aggregation results
        
    Examples:
        # Simple list
        result = aggregate_db_data(Node, limit=100)
        
        # With filters
        result = aggregate_db_data(
            BalanceHistory,
            filters={"balance_tia": {"gte": 1000}, "date": "2024-01-01"},
            limit=50
        )
        
        # With grouping and aggregation
        result = aggregate_db_data(
            Node,
            group_by=["country"],
            aggregations=[{"type": "count"}],
            order_by={"count": "desc"}
        )
        
        # Detailed balance aggregation
        result = aggregate_db_data(
            BalanceHistory,
            filters={"date": "2024-01-01"},
            aggregations=[
                {"type": "count"},
                {"type": "sum", "field": "balance_tia"},
                {"type": "avg", "field": "balance_tia"},
                {"type": "min", "field": "balance_tia"},
                {"type": "max", "field": "balance_tia"}
            ]
        )
    """
    logger.info(f"[START] DB Aggregation for {model_class.__tablename__}")
    logger.info(f"Filters: {filters}, Group by: {group_by}, Aggregations: {aggregations}")
    
    session = SessionLocal()
    try:
        # Create base query
        query = session.query(model_class)
        
        # Apply filters
        if filters:
            query = _apply_filters(query, model_class, filters)
        
        # Optimization for count_only
        if return_format == "count_only" and not (aggregations or group_by):
            # Simple count without aggregation
            count = query.count()
            return {"total": count}
        
        # Determine if aggregation is needed
        has_aggregations = aggregations and len(aggregations) > 0
        has_grouping = group_by and len(group_by) > 0
        
        if has_aggregations or has_grouping:
            # Aggregated query
            query = _apply_group_by_and_aggregations(query, model_class, group_by, aggregations)
            results = query.all()
            processed_results = _process_aggregated_results(results, group_by, aggregations)
            
            # Special handling for count_only with aggregation
            if return_format == "count_only" and aggregations:
                logger.info(f"Count only with aggregations: {processed_results}")
                # If there's only count aggregation, return its value
                for agg in aggregations:
                    if agg.get("type") == "count":
                        if processed_results and "count" in processed_results[0]:
                            logger.info(f"Returning count: {processed_results[0]['count']}")
                            return {"total": processed_results[0]["count"]}
                        break
        else:
            # Regular query
            if order_by:
                query = _apply_order_by(query, model_class, order_by)
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            
            results = query.all()
            processed_results = _process_regular_results(results, model_class)
        
        # Format result
        return _format_result(processed_results, return_format, limit, offset)
        
    except Exception as e:
        logger.error(f"Error in DB aggregation: {e}")
        return {"error": str(e), "results": [], "count": 0}
    finally:
        session.close()


def _apply_filters(query, model_class: Type, filters: Dict[str, Any]):
    """Apply filters to query"""
    for field, value in filters.items():
        if not hasattr(model_class, field):
            logger.warning(f"Field {field} not found in {model_class.__tablename__}")
            continue
        
        # Skip None values
        if value is None:
            continue
        
        column = getattr(model_class, field)
        
        if isinstance(value, dict):
            # Complex filters {operator: value}
            for operator, filter_value in value.items():
                if filter_value is None:
                    continue
                
                if operator == "eq":
                    query = query.filter(column == filter_value)
                elif operator == "ne":
                    query = query.filter(column != filter_value)
                elif operator == "gt":
                    query = query.filter(column > filter_value)
                elif operator == "gte":
                    query = query.filter(column >= filter_value)
                elif operator == "lt":
                    query = query.filter(column < filter_value)
                elif operator == "lte":
                    query = query.filter(column <= filter_value)
                elif operator == "like":
                    query = query.filter(column.like(f"%{filter_value}%"))
                elif operator == "in":
                    query = query.filter(column.in_(filter_value))
                elif operator == "not_in":
                    query = query.filter(~column.in_(filter_value))
                elif operator == "is_null":
                    if filter_value:
                        query = query.filter(column.is_(None))
                    else:
                        query = query.filter(column.isnot(None))
        else:
            # Simple filter
            query = query.filter(column == value)
    
    return query


def _apply_group_by_and_aggregations(query, model_class: Type, group_by: Optional[List[str]], aggregations: Optional[List[Dict[str, str]]]):
    """Apply grouping and aggregations"""
    select_columns = []
    
    # Add grouping fields
    if group_by:
        for field in group_by:
            if hasattr(model_class, field):
                select_columns.append(getattr(model_class, field))
    
    # Add aggregations
    if aggregations:
        for agg in aggregations:
            agg_type = agg.get("type", "count")
            field = agg.get("field")
            
            if agg_type == "count":
                select_columns.append(func.count().label("count"))
            elif field and hasattr(model_class, field):
                column = getattr(model_class, field)
                if agg_type == "sum":
                    select_columns.append(func.sum(column).label(f"sum_{field}"))
                elif agg_type == "avg":
                    select_columns.append(func.avg(column).label(f"avg_{field}"))
                elif agg_type == "min":
                    select_columns.append(func.min(column).label(f"min_{field}"))
                elif agg_type == "max":
                    select_columns.append(func.max(column).label(f"max_{field}"))
    
    # Apply grouping
    if group_by:
        group_columns = []
        for field in group_by:
            if hasattr(model_class, field):
                group_columns.append(getattr(model_class, field))
        if group_columns:
            query = query.group_by(*group_columns)
    
    # Apply SELECT
    if select_columns:
        query = query.with_entities(*select_columns)
    
    return query


def _apply_order_by(query, model_class: Type, order_by: Dict[str, str]):
    """Apply sorting"""
    for field, direction in order_by.items():
        if hasattr(model_class, field):
            column = getattr(model_class, field)
            if direction.lower() == "desc":
                query = query.order_by(column.desc())
            else:
                query = query.order_by(column.asc())
        else:
            # Maybe this is an aggregated field
            if direction.lower() == "desc":
                query = query.order_by(desc(field))
            else:
                query = query.order_by(asc(field))
    
    return query


def _process_aggregated_results(results, group_by: Optional[List[str]], aggregations: Optional[List[Dict[str, str]]]):
    """Process aggregated results"""
    processed_results = []
    logger.info(f"Processing aggregated results: {len(results)} results, group_by={group_by}, aggregations={aggregations}")
    
    for result in results:
        result_dict = {}
        field_index = 0
        
        # Convert Row to tuple for processing
        if hasattr(result, '_asdict'):
            result_tuple = tuple(result)
        else:
            result_tuple = result
        
        # Add grouping fields
        if group_by:
            for field in group_by:
                if field_index < len(result_tuple):
                    result_dict[field] = result_tuple[field_index]
                    field_index += 1
        
        # Add aggregations
        if aggregations:
            for agg in aggregations:
                if field_index < len(result_tuple):
                    agg_type = agg.get("type", "count")
                    field = agg.get("field")
                    
                    if agg_type == "count":
                        result_dict["count"] = result_tuple[field_index]
                    elif field:
                        result_dict[f"{agg_type}_{field}"] = result_tuple[field_index]
                    else:
                        result_dict[f"agg_{field_index}"] = result_tuple[field_index]
                    field_index += 1
        
        processed_results.append(result_dict)
    
    return processed_results


def _process_regular_results(results, model_class: Type):
    """Process regular results"""
    processed_results = []
    
    for result in results:
        try:
            if hasattr(result, 'to_dict'):
                processed_results.append(result.to_dict())
            else:
                # Create dictionary from object attributes
                result_dict = {}
                for column in model_class.__table__.columns:
                    value = getattr(result, column.name, None)
                    # Convert datetime and date to strings
                    if isinstance(value, (datetime, date)):
                        result_dict[column.name] = value.isoformat()
                    elif hasattr(value, '__float__') and not isinstance(value, (int, str, bool)):
                        # Convert Decimal to float
                        result_dict[column.name] = float(value)
                    else:
                        result_dict[column.name] = value
                processed_results.append(result_dict)
        except Exception as e:
            logger.error(f"Error processing result: {e}")
            processed_results.append({"error": "Could not process result"})
    
    logger.info(f"Processed aggregated results: {processed_results}")
    return processed_results


def _format_result(processed_results: List[Dict], return_format: str, limit: Optional[int], offset: Optional[int]):
    """Format result"""
    count = len(processed_results)
    logger.info(f"Formatting result: return_format={return_format}, count={count}")
    
    if return_format == "count_only":
        logger.info(f"Returning count_only: {count}")
        # If there's only one result with count, return its value
        if count == 1 and processed_results and "count" in processed_results[0]:
            return {"total": processed_results[0]["count"]}
        return {"total": count}
    elif return_format == "aggregated":
        return {
            "results": processed_results,
            "count": count
        }
    else:  # "list"
        return {
            "results": processed_results,
            "count": count,
            "limit": limit,
            "offset": offset or 0
        }


# Helper functions for convenience

def get_available_fields(model_class: Type) -> List[str]:
    """Get list of available fields for model"""
    mapper = inspect(model_class)
    return [column.name for column in mapper.columns]


def get_model_info(model_class: Type) -> Dict[str, Any]:
    """Get model information"""
    mapper = inspect(model_class)
    fields = []
    
    for column in mapper.columns:
        field_info = {
            "name": column.name,
            "type": str(column.type),
            "nullable": column.nullable,
            "primary_key": column.primary_key
        }
        fields.append(field_info)
    
    return {
        "table_name": model_class.__tablename__,
        "fields": fields
    }


# Specialized functions for frequently used operations

def get_top_records(
    model_class: Type,
    order_field: str,
    limit: int = 100,
    filters: Optional[Dict[str, Any]] = None,
    order_desc: bool = True
) -> Dict[str, Any]:
    """Get top records by specific field"""
    order_by = {order_field: "desc" if order_desc else "asc"}
    
    return aggregate_db_data(
        model_class=model_class,
        filters=filters,
        order_by=order_by,
        limit=limit
    )


def get_count_by_field(
    model_class: Type,
    group_field: str,
    filters: Optional[Dict[str, Any]] = None,
    order_by_count: bool = True
) -> Dict[str, Any]:
    """Get count of records grouped by field"""
    order_by = {"count": "desc" if order_by_count else "asc"}
    
    return aggregate_db_data(
        model_class=model_class,
        filters=filters,
        group_by=[group_field],
        aggregations=[{"type": "count"}],
        order_by=order_by
    )


def get_statistics(
    model_class: Type,
    field: str,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Get field statistics (count, sum, avg, min, max)"""
    return aggregate_db_data(
        model_class=model_class,
        filters=filters,
        aggregations=[
            {"type": "count"},
            {"type": "sum", "field": field},
            {"type": "avg", "field": field},
            {"type": "min", "field": field},
            {"type": "max", "field": field}
        ],
        return_format="aggregated"
    )
