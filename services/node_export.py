import json
import logging
from sqlalchemy.orm import Session
from models.node import Node
from services.db import engine

def export_nodes_json(out_path=None):
    """
    Export all nodes from the database as JSON string or to a file.
    Args:
        out_path (str|None): Path to the output JSON file. If None, returns JSON string.
    Returns:
        str: JSON string of all nodes.
    """
    session = Session(engine)
    try:
        nodes = session.query(Node).all()
        result = []
        for node in nodes:
            result.append({
                "id": node.id,
                "peer_id": node.peer_id,
                "ip": node.ip,
                "city": node.city,
                "region": node.region,
                "country": node.country,
                "lat": node.lat,
                "lon": node.lon,
                "provider": node.provider,
                # New fields from location.json
                "continent": node.continent,
                "updated_at": node.updated_at,
                # Rules fields from score_breakdown.rules
                "city_over_limit": node.city_over_limit,
                "country_over_limit": node.country_over_limit,
                "continent_over_limit": node.continent_over_limit,
                "provider_over_limit": node.provider_over_limit,
                "provider_hetzner": node.provider_hetzner,
            })
        js = json.dumps(result, ensure_ascii=False, indent=2)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(js)
            logging.info(f"Exported {len(result)} nodes to {out_path}")
        return js
    except Exception as e:
        logging.error(f"Failed to export nodes: {e}")
        raise
    finally:
        session.close() 