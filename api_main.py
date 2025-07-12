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
    """Get list of nodes with pagination."""
    nodes = json.loads(export_nodes_json())
    return paginate(nodes, skip, limit)

@app.get("/chain", response_model=dict, tags=["Chain"])
def get_chain(skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    """Get chain metrics (legacy) with pagination."""
    chain = json.loads(export_chain_json(limit=10000))  # get all, then paginate
    return paginate(chain, skip, limit)

@app.get("/metrics/aggregate", response_model=List[AggregatedMetricOut], tags=["Metrics"])
def get_aggregated_metrics(metric_name: str, hours: int = Query(24, ge=1, le=168)):
    """Get aggregated metrics per instance for the last N hours."""
    data = aggregate_metrics(metric_name, period_hours=hours)
    return data

@app.get("/releases", response_model=dict, tags=["Releases"])
def get_releases(skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    """Get releases with pagination."""
    releases = json.loads(export_releases_json())
    return paginate(releases, skip, limit)

@app.get("/health", tags=["Utils"])
def health():
    return {"status": "ok"}

# --- Run with: uvicorn api_main:app --reload --- 