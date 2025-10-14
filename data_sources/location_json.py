import json
import logging
from datetime import datetime
from config import LOCATION_JSON_PATH

def read_location_json(path=LOCATION_JSON_PATH):
    """
    Read location.json and return list of node dicts with normalization.
    Handles file errors and logs them.
    """
    nodes = []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        peers = data.get('peers', [])
        logging.info(f"Found {len(peers)} peers in location.json")
        
        for peer in peers:
            # Validate required fields
            peer_id = peer.get('peer_id', '').strip()
            if not peer_id:
                logging.warning("Skipping peer without peer_id")
                continue
            
            # Extract score breakdown rules
            score_breakdown = peer.get('score_breakdown', {})
            rules = score_breakdown.get('rules', {})
            
            # Normalize data for database
            node_data = {
                'peer_id': peer_id,
                'ip': peer.get('ip', '').strip() or None,
                'city': peer.get('city', '').strip() or None,
                'region': peer.get('region', '').strip() or None,
                'country': peer.get('country_code', '').strip() or None,
                'lat': peer.get('latitude'),
                'lon': peer.get('longitude'),
                'provider': peer.get('provider', '').strip() or None,
                
                # New fields from location.json
                'continent': peer.get('continent', '').strip() or None,
                'updated_at': peer.get('updated_at'),
                
                # Rules fields from score_breakdown.rules
                'city_over_limit': rules.get('city_over_limit'),
                'country_over_limit': rules.get('country_over_limit'),
                'continent_over_limit': rules.get('continent_over_limit'),
                'provider_over_limit': rules.get('provider_over_limit'),
                'provider_hetzner': rules.get('provider_hetzner'),
            }
            
            nodes.append(node_data)
            
    except FileNotFoundError:
        logging.error(f"Location JSON file not found: {path}")
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in location file: {e}")
    except Exception as e:
        logging.error(f"Error reading location JSON file: {e}")
    
    logging.info(f"Successfully processed {len(nodes)} nodes from location.json")
    return nodes
