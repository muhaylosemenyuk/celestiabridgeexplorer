from fastapi import FastAPI, Query
from typing import List, Optional
from pydantic import BaseModel
from services.node_export import export_nodes_json
from services.chain_export import export_chain_json
from services.metrics_agg import aggregate_metrics
from services.releases_export import export_releases_json
import json

app = FastAPI(title="CelestiaBridge API", description="Open API for CelestiaBridge analytics", version="1.0.0")

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

# --- API Endpoints ---
@app.get("/nodes", response_model=dict, tags=["Nodes"])
def get_nodes(skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    """
    Returns a paginated list of Celestia network nodes.

    Parameters:
      - skip (int): Number of records to skip (for pagination).
      - limit (int): Maximum number of records to return (for pagination, max 1000).

    Response:
      - total (int): Total number of nodes.
      - skip (int): Number of skipped records.
      - limit (int): Limit used for pagination.
      - items (List[NodeOut]): List of node objects, each with:
          - id (int)
          - peer_id (str)
          - ip (str, optional)
          - city (str, optional)
          - region (str, optional)
          - country (str, optional)
          - lat (float, optional)
          - lon (float, optional)
          - org (str, optional)
    """
    nodes = json.loads(export_nodes_json())
    return paginate(nodes, skip, limit)

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

@app.get("/metrics/aggregate", response_model=List[AggregatedMetricOut], tags=["Metrics"])
def get_aggregated_metrics(metric_name: str, hours: int = Query(24, ge=1, le=168)):
    """
    Returns aggregated metrics per instance for the last N hours.

    Parameters:
      - metric_name (str): Name of the metric to aggregate (e.g. "latency", "uptime").
      - hours (int): Number of hours to aggregate over (default 24, max 168).

    Response:
      - List[AggregatedMetricOut]: List of objects, each with:
          - instance (str): Instance name or ID.
          - avg (float): Average value.
          - min (float): Minimum value.
          - max (float): Maximum value.
          - count (int): Number of samples.
    """
    data = aggregate_metrics(metric_name, period_hours=hours)
    return data

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

@app.get("/health", tags=["Utils"])
def health():
    """
    Returns the health status of the API.

    Response:
      - status (str): "ok" if the API is running.
    """
    return {"status": "ok"}

# --- Run with: uvicorn api_main:app --reload --- 