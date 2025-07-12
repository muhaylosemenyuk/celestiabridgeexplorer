import csv
from config import GEO_CSV_PATH
import re
import logging
try:
    from pycountry_convert import country_alpha2_to_continent_code, convert_continent_code_to_continent_name
except ImportError:
    country_alpha2_to_continent_code = None
    convert_continent_code_to_continent_name = None

def get_continent(cc):
    """
    Return continent name by country code (ISO alpha-2). Returns '-' if not available or on error.
    """
    if not cc or not country_alpha2_to_continent_code or not convert_continent_code_to_continent_name:
        return "-"
    try:
        cont_code = country_alpha2_to_continent_code(cc.upper())
        return convert_continent_code_to_continent_name(cont_code)
    except Exception as e:
        logging.warning(f"Failed to get continent for country code '{cc}': {e}")
        return "-"

def read_geo_csv(path=GEO_CSV_PATH):
    """
    Read geo-csv and return list of node dicts with legacy normalization.
    Handles file errors and logs them.
    """
    nodes = []
    try:
        with open(path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                peer_id = row.get('peer_id', '').strip()
                ip = row.get('ip', '').strip()
                city = row.get('city', '').strip() or "-"
                country = row.get('country', '').strip().upper() or "N/A"
                org = row.get('org', '').strip()
                # Legacy: provider = org without ASxxxx prefix
                org = re.sub(r"^AS\d+\s+", "", org) or "N/A"
                # Legacy: region = continent
                region = get_continent(country)
                try:
                    lat = float(row['lat']) if row.get('lat') else None
                except Exception as e:
                    logging.warning(f"Invalid latitude value: {row.get('lat')}, error: {e}")
                    lat = None
                try:
                    lon = float(row['lon']) if row.get('lon') else None
                except Exception as e:
                    logging.warning(f"Invalid longitude value: {row.get('lon')}, error: {e}")
                    lon = None
                nodes.append({
                    'peer_id': peer_id,
                    'ip': ip,
                    'city': city,
                    'region': region,
                    'country': country,
                    'lat': lat,
                    'lon': lon,
                    'org': org,
                })
    except Exception as e:
        logging.error(f"Failed to read geo CSV file '{path}': {e}")
    return nodes
