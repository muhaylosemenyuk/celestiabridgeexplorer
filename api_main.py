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
from services.db import SessionLocal
from models.node import Node
from models.balance import BalanceHistory
from models.metric import Metric
from models.validator import Validator
from models.delegation import Delegation

logger = logging.getLogger(__name__)

# Import field validation from filter configs
from filter_configs.filter_configs import get_valid_fields, validate_field

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
    - nodes: Bridge nodes with geographic data (peer_id, ip, city, country, provider)
    - validators: Validators with staking data (operator_address, moniker, tokens, status, uptime)
    - metrics: Performance metrics from bridge nodes only (instance=peer_id from nodes table)
    - balance_history: Historical wallet balances
    - delegations: Delegation data with validator information
    - chain: Chain metrics and statistics
    - releases: Software releases
    
    NODE TYPES:
    - Validators: Participate in consensus, stored in 'validators' table
    - Bridge nodes: Provide network services, stored in 'nodes' table, metrics in 'metrics' table
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
    provider: Optional[str]
    # New fields from location.json
    continent: Optional[str]
    updated_at: Optional[str]
    # Rules fields from score_breakdown.rules
    city_over_limit: Optional[bool]
    country_over_limit: Optional[bool]
    continent_over_limit: Optional[bool]
    provider_over_limit: Optional[bool]
    provider_hetzner: Optional[bool]

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
    provider: Optional[str] = Query(None, description="Filter by provider name"),
    continent: Optional[str] = Query(None, description="Filter by continent (e.g., 'EU', 'NA', 'AS')"),
    provider_hetzner: Optional[bool] = Query(None, description="Filter by Hetzner provider (true/false)"),
    # Decentralization filters
    city_over_limit: Optional[bool] = Query(None, description="Filter by city over limit (true/false)"),
    country_over_limit: Optional[bool] = Query(None, description="Filter by country over limit (true/false)"),
    continent_over_limit: Optional[bool] = Query(None, description="Filter by continent over limit (true/false)"),
    provider_over_limit: Optional[bool] = Query(None, description="Filter by provider over limit (true/false)"),
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
    Returns Celestia bridge network nodes with filtering, grouping, and aggregation.
    Enhanced with decentralization metrics from location.json.

    FIELDS: id, peer_id, ip, city, region, country, lat, lon, provider, continent, 
            updated_at, city_over_limit, country_over_limit, continent_over_limit, 
            provider_over_limit, provider_hetzner

    FILTERS: country, region, city, provider, continent, provider_hetzner, city_over_limit, country_over_limit, continent_over_limit, provider_over_limit

    DECENTRALIZATION METRICS:
    - *_over_limit: Boolean flags indicating if limits are exceeded for geographic/provider diversity
    - provider_hetzner: Boolean flag for Hetzner provider nodes

    EXAMPLES:
    - All nodes: ?limit=100
    - Filter by country: ?country=US&limit=50
    - Filter by continent: ?continent=EU&limit=50
    - Filter by provider: ?provider=Hetzner&limit=50
    - Filter Hetzner nodes: ?provider_hetzner=true
    - Filter nodes with poor city decentralization: ?city_over_limit=true
    - Filter nodes with poor country decentralization: ?country_over_limit=true
    - Filter nodes with poor continent decentralization: ?continent_over_limit=true
    - Filter nodes with poor provider decentralization: ?provider_over_limit=true
    - Group by country: ?group_by=country&aggregations=[{"type":"count"}]&order_by=count&order_direction=desc
    - Group by continent: ?group_by=continent&aggregations=[{"type":"count"}]
    - Provider analysis: ?group_by=provider&aggregations=[{"type":"count"}]&order_by=count&order_direction=desc
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
        if provider is not None and str(type(provider)).find('Query') == -1:
            filters['provider'] = provider
        if continent is not None and str(type(continent)).find('Query') == -1:
            filters['continent'] = continent
        if provider_hetzner is not None:
            filters['provider_hetzner'] = provider_hetzner
        if city_over_limit is not None:
            filters['city_over_limit'] = city_over_limit
        if country_over_limit is not None:
            filters['country_over_limit'] = country_over_limit
        if continent_over_limit is not None:
            filters['continent_over_limit'] = continent_over_limit
        if provider_over_limit is not None:
            filters['provider_over_limit'] = provider_over_limit
        
        # Parse grouping
        group_by_list = None
        if group_by:
            group_by_list = [field.strip() for field in group_by.split(",")]
            # Validate grouping fields
            for field in group_by_list:
                if not validate_field("nodes", field):
                    return {"error": f"Invalid field '{field}' for grouping. Valid fields: {get_valid_fields('nodes')}"}
        
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
                return {"error": f"Invalid field '{order_by}' for sorting. Valid fields: {get_valid_fields('nodes')}"}
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
def get_chain(skip: int = Query(0, ge=0), limit: int = Query(1, ge=1, le=1000)):
    """
    Returns Celestia chain metrics.

    FIELDS: timestamp, staked_tokens, missed_blocks, inflation, apr, price, delegators, annual_provisions, supply

    EXAMPLES:
    - Recent metrics: ?limit=10&order_by=timestamp&order_direction=desc
    - All metrics: ?limit=1000
    """
    chain = json.loads(export_chain_json(limit=10000))  # get all, then paginate
    return paginate(chain, skip, limit)

@app.get("/metrics/aggregate", response_model=dict, tags=["Metrics"])
def get_aggregated_metrics(
    metric_name: Optional[str] = Query(None, description="Filter by specific metric name (optional)"), 
    hours: int = Query(24, ge=1, le=168),
    instance: Optional[str] = Query(None, description="Filter by specific instance"),
    min_value: Optional[float] = Query(None, description="Minimum metric value (filters raw values before aggregation)"),
    max_value: Optional[float] = Query(None, description="Maximum metric value (filters raw values before aggregation)"),
    # Post-aggregation filters (filter by aggregated values)
    min_avg_value: Optional[float] = Query(None, description="Minimum average value (filters after aggregation)"),
    max_avg_value: Optional[float] = Query(None, description="Maximum average value (filters after aggregation)"),
    min_max_value: Optional[float] = Query(None, description="Minimum max value (filters after aggregation)"),
    max_max_value: Optional[float] = Query(None, description="Maximum max value (filters after aggregation)"),
    # Node filters
    country: Optional[str] = Query(None, description="Filter by node country"),
    region: Optional[str] = Query(None, description="Filter by node region"),
    city: Optional[str] = Query(None, description="Filter by node city"),
    provider: Optional[str] = Query(None, description="Filter by node provider"),
    # Include node info
    include_node_info: bool = Query(False, description="Include node information (country, region, city, provider)"),
    # Grouping (deprecated - always groups by instance and metric_name)
    group_by: Optional[str] = Query(None, description="DEPRECATED: Grouping is always by instance and metric_name")
):
    """
    Returns aggregated metrics per bridge node instance for the last N hours with filtering.
    
    DATA SOURCE: Metrics are collected from OpenTelemetry (OTEL) endpoint and stored in database.
    Metrics are imported from OTEL endpoint (configured via OTEL_METRICS_URL) using import_metrics_to_db().
    
    AVAILABLE METRICS FROM OTEL:
    - process_runtime_go_mem_heap_alloc_bytes: Memory heap allocation in bytes
    - process_runtime_go_goroutines: Number of active goroutines
    - process_runtime_go_gc_pause_ns_sum: GC pause time sum in nanoseconds
    - eds_cache_0x40082a1b08_get_counter_total: EDS cache get counter
    - eds_store_put_time_histogram_sum: EDS store put time sum
    - eds_store_put_time_histogram_count: EDS store put time count
    - eds_store_put_time_histogram_bucket: EDS store put time histogram buckets
    - shrex_eds_server_responses_total: Share exchange EDS server responses
    - shrex_nd_server_responses_total: Share exchange ND server responses
    - hdr_sync_subjective_head_gauge: Subjective head height gauge
    - hdr_store_head_height_gauge: Store head height gauge
    - is_sync: Calculated sync percentage (0-100%) based on hdr_sync_subjective_head_gauge / hdr_store_head_height_gauge

    FIELDS: instance, metric_name, avg_value, min_value, max_value, count
    NODE FIELDS (when include_node_info=true): node_country, node_region, node_city, node_provider

    FILTERS: 
    - metric_name (optional), hours, instance
    - min_value, max_value: Filter raw metric values BEFORE aggregation
    - min_avg_value, max_avg_value: Filter aggregated average values AFTER aggregation
    - min_max_value, max_max_value: Filter aggregated max values AFTER aggregation
    - country, region, city, provider: Node location/provider filters
    
    NOTE: 
    - Metrics are collected only from bridge nodes (stored in 'nodes' table). Validators do not have metrics.
    - Metrics are imported from OpenTelemetry endpoint and stored in 'metrics' table.
    - For is_sync metric: values range from 0-100 (percentage). Use min_max_value=100&max_max_value=100 to find nodes with 100% sync.
    - Pre-aggregation filters (min_value/max_value) filter individual metric records before grouping.
    - Post-aggregation filters (min_avg_value/max_avg_value/min_max_value/max_max_value) filter aggregated results.
    
    GROUPING: Always groups by 'instance' and 'metric_name' for all cases

    EXAMPLES:
    - All bridge node metrics: ?hours=24
    - Latency metrics: ?metric_name=latency&hours=24
    - Nodes with 100% sync: ?metric_name=is_sync&min_max_value=100&max_max_value=100
    - Nodes with avg sync >= 95%: ?metric_name=is_sync&min_avg_value=95
    - Metrics by country: ?country=US&hours=24
    - With node info: ?include_node_info=true&country=DE&hours=24
    - Specific bridge node: ?instance=12D3KooW...&include_node_info=true
    """
    # Calculate time filter
    time_threshold = datetime.utcnow() - timedelta(hours=hours)
    
    # Use filter builder for metrics
    from services.filter_builder import build_filters
    from filter_configs.filter_configs import get_filter_config
    
    # Prepare parameters for filter builder
    params = locals().copy()
    params['time_threshold'] = time_threshold
    
    # Build filters using configuration
    config = get_filter_config('metrics')
    filters = build_filters(Metric, params, config)
    
    # Add node filters if provided
    node_filters = {}
    if country is not None:
        node_filters["country"] = country
    if region is not None:
        node_filters["region"] = region
    if city is not None:
        node_filters["city"] = city
    if provider is not None:
        node_filters["provider"] = provider
    
    # Handle node filters separately - they will be applied in JOIN query
    # Don't add them to main filters as they need special handling
    
    # Always group by instance and metric_name for all cases
    group_by_list = ["instance", "metric_name"]
    
    # Check if we need JOIN for node filters or include_node_info
    if include_node_info or node_filters:
        # Use custom JOIN query with node filters
        from services.db import SessionLocal
        from sqlalchemy import func
        
        session = SessionLocal()
        try:
            # Create base query with JOIN
            query = session.query(
                Metric.instance,
                Metric.metric_name,
                func.count().label('count'),
                func.avg(Metric.value).label('avg_value'),
                func.min(Metric.value).label('min_value'),
                func.max(Metric.value).label('max_value')
            ).join(Node, Metric.instance == Node.peer_id)
            
            # Apply metric filters
            if filters:
                for field, value in filters.items():
                    if hasattr(Metric, field) and value is not None:
                        if isinstance(value, dict):
                            for operator, filter_value in value.items():
                                if filter_value is not None:
                                    column = getattr(Metric, field)
                                    if operator == "eq":
                                        query = query.filter(column == filter_value)
                                    elif operator == "like":
                                        query = query.filter(column.like(f"%{filter_value}%"))
                                    elif operator == "gte":
                                        query = query.filter(column >= filter_value)
                                    elif operator == "lte":
                                        query = query.filter(column <= filter_value)
                        else:
                            column = getattr(Metric, field)
                            query = query.filter(column == value)
            
            # Apply node filters
            if node_filters:
                for field, value in node_filters.items():
                    if hasattr(Node, field) and value is not None:
                        column = getattr(Node, field)
                        query = query.filter(column == value)
            
            # Add node info fields if requested
            if include_node_info:
                query = query.add_columns(
                    Node.country.label('node_country'),
                    Node.region.label('node_region'),
                    Node.city.label('node_city'),
                    Node.provider.label('node_provider')
                )
            
            # Apply grouping and ordering
            query = query.group_by(Metric.instance, Metric.metric_name)
            if include_node_info:
                query = query.group_by(Node.country, Node.region, Node.city, Node.provider)
            
            query = query.order_by(func.count().desc())
            
            # Execute query
            results = query.all()
            
            # Process results
            processed_results = []
            for result in results:
                result_dict = {
                    'instance': result.instance,
                    'metric_name': result.metric_name,
                    'count': result.count,
                    'avg_value': float(result.avg_value) if result.avg_value is not None else None,
                    'min_value': float(result.min_value) if result.min_value is not None else None,
                    'max_value': float(result.max_value) if result.max_value is not None else None
                }
                
                if include_node_info:
                    result_dict.update({
                        'node_country': result.node_country,
                        'node_region': result.node_region,
                        'node_city': result.node_city,
                        'node_provider': result.node_provider
                    })
                
                # Apply post-aggregation filters
                if min_avg_value is not None and result_dict['avg_value'] is not None:
                    if result_dict['avg_value'] < min_avg_value:
                        continue
                if max_avg_value is not None and result_dict['avg_value'] is not None:
                    if result_dict['avg_value'] > max_avg_value:
                        continue
                if min_max_value is not None and result_dict['max_value'] is not None:
                    if result_dict['max_value'] < min_max_value:
                        continue
                if max_max_value is not None and result_dict['max_value'] is not None:
                    if result_dict['max_value'] > max_max_value:
                        continue
                
                processed_results.append(result_dict)
            
            result = {
                'results': processed_results,
                'count': len(processed_results)
            }
            
        finally:
            session.close()
    else:
        # Use regular query without JOIN
        result = aggregate_db_data(
            model_class=Metric,
            filters=filters,
            group_by=group_by_list,
            aggregations=[
                {"type": "count"},
                {"type": "avg", "field": "value"},
                {"type": "min", "field": "value"},
                {"type": "max", "field": "value"}
            ],
            order_by={"count": "desc"},
            return_format="aggregated"
        )
        
        # Apply post-aggregation filters
        if result and 'results' in result:
            filtered_results = []
            for item in result['results']:
                # Apply post-aggregation filters
                if min_avg_value is not None and item.get('avg_value') is not None:
                    if item['avg_value'] < min_avg_value:
                        continue
                if max_avg_value is not None and item.get('avg_value') is not None:
                    if item['avg_value'] > max_avg_value:
                        continue
                if min_max_value is not None and item.get('max_value') is not None:
                    if item['max_value'] < min_max_value:
                        continue
                if max_max_value is not None and item.get('max_value') is not None:
                    if item['max_value'] > max_max_value:
                        continue
                filtered_results.append(item)
            
            result['results'] = filtered_results
            result['count'] = len(filtered_results)
    
    return result

@app.get("/releases", response_model=dict, tags=["Releases"])
def get_releases(
    skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    network: Optional[str] = Query(None, description="Filter by network type: mainnet or testnet")
):
    """
    Returns Celestia software releases.

    FIELDS: version, published_at, announce_str, deadline_str, network
    
    NETWORK FIELD:
    - mainnet: Production releases without any suffixes (clean version numbers)
    - testnet: Releases with any suffixes (including -mocha, -arabica, -rc, -alpha, -beta, etc.)
    
    FILTERS:
    - network: Filter by network type (mainnet or testnet)
    
    EXAMPLES:
    - GET /releases - Get all releases (paginated)
    - GET /releases?network=mainnet - Get only mainnet releases
    - GET /releases?network=testnet - Get only testnet releases
    - GET /releases?limit=5 - Get first 5 releases
    - GET /releases?network=testnet&limit=10 - Get first 10 testnet releases
    - GET /releases?skip=20&limit=10 - Get releases 21-30 (pagination)
    """
    releases = json.loads(export_releases_json(network=network))
    return paginate(releases, skip, limit)

# --- Universal Balance Endpoint ---
@app.get("/balances", response_model=dict, tags=["Balances"])
def get_balances(
    # Pagination
    skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"), 
    limit: int = Query(10, ge=1, le=1000, description="Maximum number of records to return"),
    # Filters
    target_date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    min_balance: Optional[float] = Query(None, description="Minimum balance in TIA"),
    max_balance: Optional[float] = Query(None, description="Maximum balance in TIA"),
    address: Optional[str] = Query(None, description="Specific wallet address"),
    include_zero_balances: bool = Query(False, description="Include addresses with zero balances"),
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
    Returns balance data with filtering, grouping, and aggregation.

    FIELDS: id, address, date, balance_tia

    FILTERS: address, target_date, min_balance, max_balance

    EXAMPLES:
    - Top wallets: ?limit=100&order_by=balance_tia&order_direction=desc
    - Filter by balance: ?min_balance=1000&limit=50
    - Date statistics: ?target_date=2024-01-01&aggregations=[{"type":"count"},{"type":"sum","field":"balance_tia"}]
    - Group by date: ?group_by=date&aggregations=[{"type":"count"}]&order_by=date&order_direction=desc
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
            # Handle both string and dict formats for address
            if isinstance(address, dict) and "in" in address:
                # Multiple addresses with 'in' filter
                filters["address"] = address
            else:
                # Single address or comma-separated string
                if isinstance(address, str) and "," in address:
                    # Split comma-separated addresses and use 'in' filter
                    addresses = [addr.strip() for addr in address.split(",") if addr.strip()]
                    if addresses:
                        filters["address"] = {"in": addresses}
                else:
                    # Single address - use exact match
                    filters["address"] = address
        
        # By default filter out zero balances unless explicitly requested
        if not include_zero_balances and (min_balance is None or min_balance <= 0):
            filters["balance_tia"] = {**filters.get("balance_tia", {}), "gt": 0}
        
        # Parse grouping
        group_by_list = None
        if group_by:
            group_by_list = [field.strip() for field in group_by.split(",")]
            # Validate grouping fields
            for field in group_by_list:
                if not validate_field("balances", field):
                    return {"error": f"Invalid field '{field}' for grouping. Valid fields: {get_valid_fields('balances')}"}
        
        # Parse aggregations
        parsed_aggregations = None
        if aggregations:
            try:
                parsed_aggregations = json.loads(aggregations)
            except json.JSONDecodeError:
                return {"error": "Invalid JSON in aggregations parameter"}
        
        # Build sorting
        if not validate_field("balances", order_by):
            return {"error": f"Invalid field '{order_by}' for sorting. Valid fields: {get_valid_fields('balances')}"}
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


# ===== VALIDATORS ENDPOINTS =====

@app.get("/validators", response_model=dict, tags=["Validators"])
def get_validators(
    # Pagination
    skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"), 
    limit: int = Query(10, ge=1, le=1000, description="Maximum number of records to return"),
    
    # Filters - Basic identifiers
    operator_address: Optional[str] = Query(None, description="Filter by operator address (partial match)"),
    consensus_address: Optional[str] = Query(None, description="Filter by consensus address (partial match)"),
    
    # Filters - Validator description
    moniker: Optional[str] = Query(None, description="Filter by moniker (partial match)"),
    
    # Filters - Status and state
    status: Optional[str] = Query(None, description="Filter by validator status (e.g., 'BOND_STATUS_BONDED', 'BONDED', 'UNBONDED', 'UNBONDING')"),
    jailed: Optional[bool] = Query(None, description="Filter by jailed status"),
    
    # Filters - Tokens
    min_tokens: Optional[float] = Query(None, ge=0, description="Minimum tokens amount"),
    max_tokens: Optional[float] = Query(None, ge=0, description="Maximum tokens amount"),
    
    # Filters - Commission
    commission_rate: Optional[float] = Query(None, ge=0, le=1, description="Exact commission rate (0.0-1.0)"),
    min_commission_rate: Optional[float] = Query(None, ge=0, le=1, description="Minimum commission rate"),
    max_commission_rate: Optional[float] = Query(None, ge=0, le=1, description="Maximum commission rate"),
    
    # Filters - Consensus metrics
    min_voting_power: Optional[float] = Query(None, ge=0, description="Minimum voting power"),
    max_voting_power: Optional[float] = Query(None, ge=0, description="Maximum voting power"),
    
    # Filters - Uptime metrics
    min_uptime: Optional[float] = Query(None, ge=0, le=100, description="Minimum uptime percentage"),
    max_uptime: Optional[float] = Query(None, ge=0, le=100, description="Maximum uptime percentage"),
    min_missed_blocks: Optional[int] = Query(None, ge=0, description="Minimum missed blocks counter"),
    max_missed_blocks: Optional[int] = Query(None, ge=0, description="Maximum missed blocks counter"),
    
    # Filters - Delegation statistics
    min_total_delegations: Optional[int] = Query(None, ge=0, description="Minimum total delegations"),
    max_total_delegations: Optional[int] = Query(None, ge=0, description="Maximum total delegations"),
    min_total_delegators: Optional[int] = Query(None, ge=0, description="Minimum total delegators"),
    max_total_delegators: Optional[int] = Query(None, ge=0, description="Maximum total delegators"),
    
    
    # Grouping
    group_by: Optional[str] = Query(None, description="Comma-separated fields to group by (e.g., 'status,jailed')"),
    
    # Aggregations
    aggregations: Optional[str] = Query(None, description="JSON string with aggregations: [{'type': 'count'}, {'type': 'sum', 'field': 'field_name'}]"),
    
    # Sorting
    order_by: Optional[str] = Query(None, description="Field to sort by"),
    order_direction: Optional[str] = Query("desc", description="Sort direction (asc, desc)"),
    
    # Return format
    return_format: str = Query("list", description="Return format: list, aggregated, count_only")
):
    """
    Returns Celestia validators with filtering, grouping, and aggregation.

    FIELDS: id, operator_address, moniker, status, jailed, tokens, commission_rate, voting_power, uptime_percent, missed_blocks_counter, total_delegations, total_delegators

    FILTERS: moniker, status, jailed, min_tokens, max_tokens, min_commission_rate, max_commission_rate, min_uptime, max_uptime, min_voting_power, max_voting_power, min_missed_blocks, max_missed_blocks, min_total_delegators, max_total_delegators

    EXAMPLES:
    - Top validators: ?limit=10&order_by=tokens&order_direction=desc
    - Filter by status: ?status=BOND_STATUS_BONDED&min_tokens=1000000
    - Group and count: ?group_by=status&aggregations=[{"type":"count"}]
    - Commission range: ?min_commission_rate=0.05&max_commission_rate=0.1
    - High uptime: ?min_uptime=99.0&order_by=voting_power&order_direction=desc
    - Missed blocks: ?min_missed_blocks=0&max_missed_blocks=10&order_by=missed_blocks_counter&order_direction=asc
    - Validator missed blocks: ?operator_address=celestiavaloper1abc&order_by=missed_blocks_counter
    """
    try:
        # Use unified filter system
        from services.filter_builder import build_filters
        from filter_configs.filter_configs import get_filter_config
        
        # Build filters using configuration
        config = get_filter_config('validators')
        
        # Build filters using configuration
        filters = build_filters(Validator, locals(), config)
        
        # Validate and map status values
        if status:
            status_mapping = {
                'BONDED': 'BOND_STATUS_BONDED',
                'UNBONDED': 'BOND_STATUS_UNBONDED', 
                'UNBONDING': 'BOND_STATUS_UNBONDING',
                'BOND_STATUS_BONDED': 'BOND_STATUS_BONDED',
                'BOND_STATUS_UNBONDED': 'BOND_STATUS_UNBONDED',
                'BOND_STATUS_UNBONDING': 'BOND_STATUS_UNBONDING'
            }
            
            if status in status_mapping:
                # Update the status in filters
                mapped_status = status_mapping[status]
                if 'status' in filters:
                    filters['status'] = mapped_status
                else:
                    filters['status'] = mapped_status
            else:
                return {"error": f"Invalid status '{status}'. Valid values: {list(status_mapping.keys())}"}
        
        # Parse grouping
        group_by_list = None
        if group_by:
            group_by_list = [field.strip() for field in group_by.split(",")]
            # Validate grouping fields
            for field in group_by_list:
                if not validate_field("validators", field):
                    return {"error": f"Invalid field '{field}' for grouping. Valid fields: {get_valid_fields('validators')}"}
        
        # Parse aggregations
        parsed_aggregations = None
        if aggregations:
            try:
                parsed_aggregations = json.loads(aggregations)
            except json.JSONDecodeError:
                return {"error": "Invalid JSON in aggregations parameter"}
        
        # Build sorting
        if order_by and not validate_field("validators", order_by):
            return {"error": f"Invalid field '{order_by}' for sorting. Valid fields: {get_valid_fields('validators')}"}
        order_by_dict = {order_by: order_direction} if order_by else None
        
        # Add custom filter for None values when sorting by fields that can have None
        if order_by and order_by in ['uptime_percent', 'voting_power']:
            from services.filter_builder import FilterBuilder
            if filters is None:
                filters = {}
            filter_builder = FilterBuilder(Validator)
            # Add existing filters to the builder
            for key, value in filters.items():
                if isinstance(value, dict):
                    for op, val in value.items():
                        if op == 'like':
                            filter_builder.add_like(key, val)
                        elif op == 'ne':
                            filter_builder.add_custom(key, {'ne': val})
                        else:
                            filter_builder.add_custom(key, {op: val})
                else:
                    filter_builder.add_exact(key, value)
            
            # Add filters for active validators when sorting by uptime_percent or voting_power
            filter_builder.add_custom(order_by, {'is_null': False})  # Exclude None values
            
            filters = filter_builder.get_filters()
        
        # Use universal aggregator
        return aggregate_db_data(
            model_class=Validator,
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


# ===== DELEGATIONS ENDPOINTS =====

@app.get("/delegations", response_model=dict, tags=["Delegations"])
def get_delegations(
    # Pagination
    skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"), 
    limit: int = Query(10, ge=1, le=1000, description="Maximum number of records to return"),
    
    # Filters - Basic identifiers
    delegator_address: Optional[str] = Query(None, description="Filter by delegator address (partial match)"),
    validator_address: Optional[str] = Query(None, description="Filter by validator address (partial match)"),
    
    # Filters - Amounts
    min_amount: Optional[float] = Query(None, ge=0, description="Minimum delegation amount in TIA"),
    max_amount: Optional[float] = Query(None, ge=0, description="Maximum delegation amount in TIA"),
    
    # Filters - Dates
    target_date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    min_date: Optional[str] = Query(None, description="Minimum date in YYYY-MM-DD format"),
    max_date: Optional[str] = Query(None, description="Maximum date in YYYY-MM-DD format"),
    
    # Include validator info
    include_validator_info: bool = Query(False, description="Include validator information (moniker, status, etc.)"),
    
    # Filter options
    include_zero_delegations: bool = Query(False, description="Include zero delegations (undelegated records)"),
    
    # Filters - Validator info (only when include_validator_info=True)
    validator_moniker: Optional[str] = Query(None, description="Filter by validator moniker (partial match)"),
    validator_status: Optional[str] = Query(None, description="Filter by validator status"),
    validator_jailed: Optional[bool] = Query(None, description="Filter by validator jailed status"),
    
    # Grouping
    group_by: Optional[str] = Query(None, description="Comma-separated fields to group by (e.g., 'delegator_address,validator_address')"),
    
    # Aggregations
    aggregations: Optional[str] = Query(None, description="JSON string with aggregations: [{'type': 'count'}, {'type': 'sum', 'field': 'amount_tia'}]"),
    
    # Sorting
    order_by: Optional[str] = Query("amount_tia", description="Field to sort by (valid: id, delegator_address, validator_address, amount_tia, date, validator_moniker, validator_tokens, validator_status)"),
    order_direction: Optional[str] = Query("desc", description="Sort direction (asc, desc)"),
    
    # Return format
    return_format: str = Query("list", description="Return format: list, aggregated, count_only")
):
    """
    Returns delegation records with filtering, grouping, and aggregation.

    🚀 AUTOMATIC LATEST RECORDS: By default, returns only the latest delegation records (is_latest=True) unless date filters are specified.

    FIELDS: id, delegator_address, validator_address, amount_tia, date, is_latest

    FILTERS: delegator_address, validator_address, min_amount, max_amount, target_date, min_date, max_date, include_validator_info, validator_moniker, validator_status, validator_jailed

    EXAMPLES:
    - Top delegations: ?limit=10&order_by=amount_tia&order_direction=desc
    - Filter by delegator: ?delegator_address=celestia1abc
    - Date range: ?min_date=2024-01-01&max_date=2024-01-31
    - With validator info: ?include_validator_info=true&validator_moniker=Staker
    - Group by validator: ?group_by=validator_address&aggregations=[{"type":"count"},{"type":"sum","field":"amount_tia"}]
    """
    
    try:
        from models.delegation import Delegation
        from models.validator import Validator
        from services.filter_builder import build_filters
        from filter_configs.filter_configs import get_filter_config
        
        # Build filters using configuration
        config = get_filter_config('delegations')
        filters = build_filters(Delegation, locals(), config)
        
        # Add validator filters if include_validator_info is True
        if include_validator_info:
            validator_filters = {}
            if validator_moniker:
                validator_filters['moniker'] = {'like': validator_moniker}
            if validator_status:
                validator_filters['status'] = validator_status
            if validator_jailed is not None:
                validator_filters['jailed'] = validator_jailed
            
            if validator_filters:
                filters['validator'] = validator_filters
        
        # Parse grouping
        group_by_list = None
        if group_by:
            group_by_list = [field.strip() for field in group_by.split(",")]
            # Validate grouping fields
            for field in group_by_list:
                if not validate_field("delegations", field):
                    return {"error": f"Invalid field '{field}' for grouping. Valid fields: {get_valid_fields('delegations')}"}
        
        # Parse aggregations
        parsed_aggregations = None
        if aggregations:
            try:
                parsed_aggregations = json.loads(aggregations)
            except json.JSONDecodeError:
                return {"error": "Invalid JSON in aggregations parameter"}
        
        # Build sorting
        if order_by and not validate_field("delegations", order_by):
            return {"error": f"Invalid field '{order_by}' for sorting. Valid fields: {get_valid_fields('delegations')}"}
        order_by_dict = {order_by: order_direction} if order_by else None
        
        # Use universal aggregator
        if include_validator_info:
            # Configure JOIN fields for validator info
            join_fields = {
                'join_model': Validator,
                'join_condition': Delegation.validator_id == Validator.id,
                'fields': [
                    {'field': 'moniker', 'label': 'validator_moniker'},
                    {'field': 'status', 'label': 'validator_status'},
                    {'field': 'tokens', 'label': 'validator_tokens'},
                    {'field': 'commission_rate', 'label': 'validator_commission_rate'},
                    {'field': 'uptime_percent', 'label': 'validator_uptime_percent'}
                ]
            }
            
            # Add validator filters to main filters
            if validator_moniker:
                filters = filters or {}
                filters['validator_moniker'] = {'like': validator_moniker}
            if validator_status:
                filters = filters or {}
                filters['validator_status'] = validator_status
            if validator_jailed is not None:
                filters = filters or {}
                filters['validator_jailed'] = validator_jailed
            
            return aggregate_db_data(
                model_class=Delegation,
                filters=filters,
                group_by=group_by_list,
                aggregations=parsed_aggregations,
                order_by=order_by_dict,
                limit=limit,
                offset=skip,
                return_format=return_format,
                join_fields=join_fields
            )
        else:
            # Simple query without JOIN
            return aggregate_db_data(
                model_class=Delegation,
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
