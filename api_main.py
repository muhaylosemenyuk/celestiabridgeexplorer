import json
import logging
import uvicorn
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Import services
from services.chain_export import export_chain_json
from services.releases_export import export_releases_json
from services.universal_db_aggregator import aggregate_db_data
from models.node import Node
from models.balance import BalanceHistory
from models.metric import Metric

# Logging setup
logger = logging.getLogger(__name__)

# Valid fields for models
VALID_FIELDS = {
    "nodes": ["id", "peer_id", "ip", "city", "region", "country", "lat", "lon", "org"],
    "balance_history": ["id", "address", "date", "balance_tia"],
    "metrics": ["id", "metric_name", "value", "timestamp", "instance"],
    "chain": ["id", "height", "timestamp", "block_time", "tx_count"]
}

def validate_field(model_name: str, field: str) -> bool:
    """Check if field exists in model"""
    return field in VALID_FIELDS.get(model_name, [])

app = FastAPI(
    title="CelestiaBridge Explorer API",
    description="""
    API for exploring Celestia blockchain data with advanced aggregation capabilities.
    
    AGGREGATOR FEATURES:
    - Filtering: Simple filters (field=value) and complex filters (field={"operator": value})
    - Grouping: Group results by one or more fields
    - Aggregation: count, sum, avg, min, max operations
    - Sorting: Sort by any field in ascending or descending order
    - Pagination: Skip and limit results
    - Return formats: list, aggregated, count_only
    
    SUPPORTED OPERATORS for complex filters:
    - eq: equals
    - ne: not equals  
    - gt: greater than
    - gte: greater than or equal
    - lt: less than
    - lte: less than or equal
    - like: pattern matching (SQL LIKE)
    - in: value in list
    - not_in: value not in list
    - is_null: check for null values
    
    AVAILABLE TABLES:
    - nodes: Celestia network nodes with geographic data
    - balance_history: Historical wallet balances
    - chain: Chain metrics and statistics
    - metrics: Performance metrics
    - releases: Software releases
    """,
    version="1.0.0"
)

# --- Pydantic Schemas ---
class NodeOut(BaseModel):
    id: int
    peer_id: str
    ip: Optional[str]
    city: Optional[str]
    region: Optional[str]
    country: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    org: Optional[str]

class ChainOut(BaseModel):
    timestamp: Optional[str]
    staked_tokens: Optional[float]
    missed_blocks: Optional[int]
    inflation: Optional[float]
    apr: Optional[float]
    price: Optional[float]
    delegators: Optional[int]
    annual_provisions: Optional[float]
    supply: Optional[float]

class AggregatedMetricOut(BaseModel):
    instance: str
    avg: float
    min: float
    max: float
    count: int

class ReleaseOut(BaseModel):
    version: str
    published_at: Optional[str]
    announce_str: Optional[str]
    deadline_str: Optional[str]

# --- Pagination helper ---
def paginate(data: List[dict], skip: int, limit: int):
    total = len(data)
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "items": data[skip:skip+limit]
    }

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_model=dict, tags=["Health"])
def health_check():
    """
    Health check endpoint.
    
    Response:
      - status (str): "ok" if the API is running.
    """
    return {"status": "ok"}

# --- Other Endpoints ---
@app.get("/nodes", response_model=dict, tags=["Nodes"])
def get_nodes(
    # Pagination
    skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"), 
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    # Filters
    country: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'US')"),
    region: Optional[str] = Query(None, description="Filter by region (e.g., 'Europe', 'Asia')"),
    city: Optional[str] = Query(None, description="Filter by city name"),
    org: Optional[str] = Query(None, description="Filter by organization/provider name"),
    # Grouping
    group_by: Optional[str] = Query(None, description="Comma-separated fields to group by (e.g., 'country,region')"),
    # Aggregations
    aggregations: Optional[str] = Query(None, description="JSON string with aggregations: [{'type': 'count'}, {'type': 'sum', 'field': 'field_name'}]"),
    # Sorting
    order_by: Optional[str] = Query(None, description="Field to sort by"),
    order_direction: Optional[str] = Query("asc", description="Sort direction (asc, desc)"),
    # Return format
    return_format: str = Query("list", description="Return format: list, aggregated, count_only")
):
    """
    Returns a list of Celestia network nodes with advanced filtering, grouping, and aggregation.

    TABLE SCHEMA (nodes):
      - id (int): Primary key
      - peer_id (str): Unique peer identifier
      - ip (str): IP address
      - city (str): City name
      - region (str): Region name
      - country (str): Country code (e.g., "US", "DE")
      - lat (float): Latitude coordinate
      - lon (float): Longitude coordinate
      - org (str): Organization/provider name

    Parameters:
      - skip (int): Number of records to skip (for pagination).
      - limit (int): Maximum number of records to return (for pagination, max 1000).
      - country (str, optional): Filter by country code (e.g., "DE", "US").
      - region (str, optional): Filter by region (e.g., "Europe", "Asia").
      - city (str, optional): Filter by city name.
      - org (str, optional): Filter by organization/provider name.
      - group_by (str, optional): Comma-separated fields to group by (e.g., "country,region").
      - aggregations (str, optional): JSON string with aggregations. Examples:
        * Count: [{"type": "count"}]
        * Multiple: [{"type": "count"}, {"type": "sum", "field": "field_name"}]
        * Types: count, sum, avg, min, max
      - order_by (str, optional): Field to sort by.
      - order_direction (str, optional): Sort direction (asc, desc).
      - return_format (str): Return format (list, aggregated, count_only).

    USAGE EXAMPLES:
      # Get all nodes
      GET /nodes?limit=100
      
      # Filter by country
      GET /nodes?country=US&limit=50
      
      # Group by country and count
      GET /nodes?group_by=country&aggregations=[{"type":"count"}]&order_by=count&order_direction=desc
      
      # Get nodes by region with coordinates
      GET /nodes?region=Europe&order_by=lat&order_direction=asc

    Response:
      - results (List): List of node objects or aggregated results.
      - count (int): Number of results returned.
      - limit (int): Limit used for pagination.
      - offset (int): Offset used for pagination.
    """
    try:
        # Build filters
        filters = {}
        if country is not None and str(type(country)).find('Query') == -1:
            filters['country'] = country
        if region is not None and str(type(region)).find('Query') == -1:
            filters['region'] = region
        if city is not None and str(type(city)).find('Query') == -1:
            filters['city'] = city
        if org is not None and str(type(org)).find('Query') == -1:
            filters['org'] = org
        
        # Parse grouping
        group_by_list = None
        if group_by:
            group_by_list = [field.strip() for field in group_by.split(",")]
            # Validate grouping fields
            for field in group_by_list:
                if not validate_field("nodes", field):
                    return {"error": f"Invalid field '{field}' for grouping. Valid fields: {VALID_FIELDS['nodes']}"}
        
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
            if not validate_field("nodes", order_by):
                return {"error": f"Invalid field '{order_by}' for sorting. Valid fields: {VALID_FIELDS['nodes']}"}
            order_by_dict = {order_by: order_direction}
        
        # Use universal aggregator
        return aggregate_db_data(
            model_class=Node,
            filters=filters if filters else None,
            group_by=group_by_list,
            aggregations=parsed_aggregations,
            order_by=order_by_dict,
            limit=limit,
            offset=skip,
            return_format=return_format
        )
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/chain", response_model=dict, tags=["Chain"])
def get_chain(skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    """
    Returns a paginated list of Celestia chain metrics.

    Parameters:
      - skip (int): Number of records to skip (for pagination).
      - limit (int): Maximum number of records to return (for pagination, max 1000).

    Response:
      - total (int): Total number of records.
      - skip (int): Number of skipped records.
      - limit (int): Limit used for pagination.
      - items (List[ChainOut]): List of chain metric objects, each with:
          - timestamp (str, optional)
          - staked_tokens (float, optional)
          - missed_blocks (int, optional)
          - inflation (float, optional)
          - apr (float, optional)
          - price (float, optional)  # TIA token price here!
          - delegators (int, optional)
          - annual_provisions (float, optional)
          - supply (float, optional)
    """
    chain = json.loads(export_chain_json(limit=10000))  # get all, then paginate
    return paginate(chain, skip, limit)

@app.get("/metrics/aggregate", response_model=dict, tags=["Metrics"])
def get_aggregated_metrics(
    metric_name: str, 
    hours: int = Query(24, ge=1, le=168),
    instance: Optional[str] = Query(None, description="Filter by specific instance"),
    min_value: Optional[float] = Query(None, description="Minimum metric value"),
    max_value: Optional[float] = Query(None, description="Maximum metric value")
):
    """
    Returns aggregated metrics per instance for the last N hours with filtering.

    Parameters:
      - metric_name (str): Name of the metric to aggregate (e.g. "latency", "uptime").
      - hours (int): Number of hours to aggregate over (default 24, max 168).
      - instance (str, optional): Filter by specific instance.
      - min_value (float, optional): Minimum metric value filter.
      - max_value (float, optional): Maximum metric value filter.

    Response:
      - results (List): List of aggregated metric objects, each with:
          - instance (str): Instance name or ID.
          - avg_value (float): Average value.
          - min_value (float): Minimum value.
          - max_value (float): Maximum value.
          - count (int): Number of samples.
      - count (int): Number of results returned.
    """
    # Calculate time filter
    time_threshold = datetime.utcnow() - timedelta(hours=hours)
    
    # Build filters
    filters = {
        "metric_name": metric_name,
        "timestamp": {"gte": time_threshold}
    }
    
    if instance is not None:
        filters["instance"] = instance
    if min_value is not None:
        filters["value"] = {"gte": min_value}
    if max_value is not None:
        filters["value"] = {**filters.get("value", {}), "lte": max_value}
    
    # Use new universal aggregator
    result = aggregate_db_data(
        model_class=Metric,
        filters=filters,
        group_by=["instance"],
        aggregations=[
            {"type": "count"},
            {"type": "avg", "field": "value"},
            {"type": "min", "field": "value"},
            {"type": "max", "field": "value"}
        ],
        order_by={"count": "desc"},
        return_format="aggregated"
    )
    
    return result

@app.get("/releases", response_model=dict, tags=["Releases"])
def get_releases(skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    """
    Returns a paginated list of Celestia software releases.

    Parameters:
      - skip (int): Number of records to skip (for pagination).
      - limit (int): Maximum number of records to return (for pagination, max 1000).

    Response:
      - total (int): Total number of releases.
      - skip (int): Number of skipped records.
      - limit (int): Limit used for pagination.
      - items (List[ReleaseOut]): List of release objects, each with:
          - version (str)
          - published_at (str, optional)
          - announce_str (str, optional)
          - deadline_str (str, optional)
    """
    releases = json.loads(export_releases_json())
    return paginate(releases, skip, limit)

# --- Universal Balance Endpoint ---
@app.get("/balances", response_model=dict, tags=["Balances"])
def get_balances(
    # Pagination
    skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"), 
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    # Filters
    target_date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    min_balance: Optional[float] = Query(None, description="Minimum balance in TIA"),
    max_balance: Optional[float] = Query(None, description="Maximum balance in TIA"),
    address: Optional[str] = Query(None, description="Specific wallet address"),
    # Grouping
    group_by: Optional[str] = Query(None, description="Comma-separated fields to group by (e.g., 'date,address')"),
    # Aggregations
    aggregations: Optional[str] = Query(None, description="JSON string with aggregations: [{'type': 'count'}, {'type': 'sum', 'field': 'balance_tia'}]"),
    # Sorting
    order_by: Optional[str] = Query("balance_tia", description="Field to sort by (valid: id, address, date, balance_tia)"),
    order_direction: Optional[str] = Query("desc", description="Sort direction (asc, desc)"),
    # Return format
    return_format: str = Query("list", description="Return format: list, aggregated, count_only")
):
    """
    Returns balance data with advanced filtering, grouping, and aggregation.

    TABLE SCHEMA (balance_history):
      - id (int): Primary key
      - address (str): Wallet address (255 chars max)
      - date (date): Date in YYYY-MM-DD format
      - balance_tia (decimal): Balance in TIA tokens (20,6 precision)
      - created_at (datetime): Record creation timestamp

    Parameters:
      - skip (int): Number of records to skip (for pagination).
      - limit (int): Maximum number of records to return (for pagination, max 1000).
      - target_date (str, optional): Date in YYYY-MM-DD format.
      - min_balance (float, optional): Minimum balance in TIA.
      - max_balance (float, optional): Maximum balance in TIA.
      - address (str, optional): Specific wallet address.
      - group_by (str, optional): Comma-separated fields to group by (e.g., "date,address").
      - aggregations (str, optional): JSON string with aggregations. Examples:
        * Count: [{"type": "count"}]
        * Detailed: [{"type": "count"}, {"type": "sum", "field": "balance_tia"}, {"type": "avg", "field": "balance_tia"}]
        * Types: count, sum, avg, min, max
      - order_by (str, optional): Field to sort by (default: balance_tia).
      - order_direction (str, optional): Sort direction (asc, desc).
      - return_format (str): Return format (list, aggregated, count_only).

    USAGE EXAMPLES:
      # Get top 100 wallets by balance
      GET /balances?limit=100&order_by=balance_tia&order_direction=desc
      
      # Get wallets with balance > 1000 TIA
      GET /balances?min_balance=1000&limit=50
      
      # Get balance statistics for specific date
      GET /balances?target_date=2024-01-01&aggregations=[{"type":"count"},{"type":"sum","field":"balance_tia"},{"type":"avg","field":"balance_tia"}]&return_format=aggregated
      
      # Get specific wallet history
      GET /balances?address=celestia1abc...&order_by=date&order_direction=desc
      
      # Group by date and count wallets
      GET /balances?group_by=date&aggregations=[{"type":"count"}]&order_by=date&order_direction=desc

    Response:
      - results (List): List of balance objects or aggregated results.
      - count (int): Number of results returned.
      - limit (int): Limit used for pagination.
      - offset (int): Offset used for pagination.
    """
    try:
        # Build filters
        filters = {}
        if target_date:
            filters["date"] = target_date
        if min_balance is not None:
            filters["balance_tia"] = {"gte": min_balance}
        if max_balance is not None:
            filters["balance_tia"] = {**filters.get("balance_tia", {}), "lte": max_balance}
        if address:
            filters["address"] = address
        
        # By default filter out zero balances
        if min_balance is None or min_balance <= 0:
            filters["balance_tia"] = {**filters.get("balance_tia", {}), "gt": 0}
        
        # Parse grouping
        group_by_list = None
        if group_by:
            group_by_list = [field.strip() for field in group_by.split(",")]
            # Validate grouping fields
            for field in group_by_list:
                if not validate_field("balance_history", field):
                    return {"error": f"Invalid field '{field}' for grouping. Valid fields: {VALID_FIELDS['balance_history']}"}
        
        # Parse aggregations
        parsed_aggregations = None
        if aggregations:
            try:
                parsed_aggregations = json.loads(aggregations)
            except json.JSONDecodeError:
                return {"error": "Invalid JSON in aggregations parameter"}
        
        # Build sorting
        if not validate_field("balance_history", order_by):
            return {"error": f"Invalid field '{order_by}' for sorting. Valid fields: {VALID_FIELDS['balance_history']}"}
        order_by_dict = {order_by: order_direction}
        
        # Use universal aggregator
        return aggregate_db_data(
            model_class=BalanceHistory,
            filters=filters if filters else None,
            group_by=group_by_list,
            aggregations=parsed_aggregations,
            order_by=order_by_dict,
            limit=limit,
            offset=skip,
            return_format=return_format
        )
        
    except Exception as e:
        return {"error": str(e)}




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
