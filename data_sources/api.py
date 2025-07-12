import requests
import time
import logging
from config import POOL_URL, SLASHING_URL, ANNUAL_PROVISIONS_URL, SUPPLY_URL, COINGECKO_URL, GITHUB_RELEASES_URL, VALIDATORS_URL, API_DELEGATORS_URL

RETRIES = 3
DELAY = 5

def fetch_json(url):
    """
    Fetch JSON data from a given URL. Logs and raises exceptions on failure.
    """
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"Failed to fetch JSON from '{url}': {e}")
        raise

def fetch_json_with_retry(url, retries=RETRIES, delay=DELAY):
    """
    Fetch JSON from URL with retries and delay. Logs errors and returns None on failure.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.warning(f"Error fetching {url}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error("Max retries reached. Skipping this endpoint.")
                return None

def get_staked_tokens():
    """
    Get the number of staked tokens from the staking pool API.
    Returns: float or None
    """
    try:
        data = fetch_json(POOL_URL)
        return float(data['pool']['bonded_tokens']) / 1_000_000 if 'pool' in data and 'bonded_tokens' in data['pool'] else None
    except Exception as e:
        logging.error(f"Failed to get staked tokens: {e}")
        return None

def get_all_missed_blocks():
    """
    Get the total number of missed blocks from the slashing API.
    Returns: int
    """
    try:
        data = fetch_json(SLASHING_URL)
        total = 0
        if 'info' in data:
            for info in data['info']:
                total += int(info.get('missed_blocks_counter', 0))
        return total
    except Exception as e:
        logging.error(f"Failed to get missed blocks: {e}")
        return 0

def get_annual_provisions():
    """
    Get the annual provisions value from the mint API.
    Returns: float or None
    """
    try:
        data = fetch_json(ANNUAL_PROVISIONS_URL)
        return float(data['annual_provisions']) if 'annual_provisions' in data else None
    except Exception as e:
        logging.error(f"Failed to get annual provisions: {e}")
        return None

def get_supply():
    """
    Get the total supply from the supply API.
    Returns: float or None
    """
    try:
        data = fetch_json(SUPPLY_URL)
        return float(data['amount']['amount']) if 'amount' in data and 'amount' in data['amount'] else None
    except Exception as e:
        logging.error(f"Failed to get supply: {e}")
        return None

def get_tia_price():
    """
    Get the current TIA price in USD from CoinGecko API.
    Returns: float (default 1.0 if not available)
    """
    try:
        data = fetch_json(COINGECKO_URL)
        return data.get('celestia', {}).get('usd', 1.0)
    except Exception as e:
        logging.error(f"Failed to get TIA price: {e}")
        return 1.0

def get_github_releases():
    """
    Get the list of releases from the GitHub API.
    Returns: list or None
    """
    try:
        return fetch_json(GITHUB_RELEASES_URL)
    except Exception as e:
        logging.error(f"Failed to get GitHub releases: {e}")
        return None

def get_all_validators_with_pagination():
    """
    Get all validators from the API with pagination support.
    Returns: list of validator dicts.
    """
    validators = []
    next_key = None
    while True:
        url = VALIDATORS_URL
        if next_key:
            url += f"&pagination.key={next_key}"
        data = fetch_json_with_retry(url)
        if not data or 'validators' not in data:
            break
        validators.extend(data['validators'])
        next_key = data.get('pagination', {}).get('next_key')
        if not next_key:
            break
    return validators

def get_delegators_count(valoper):
    """
    Get the number of delegators for a given validator (valoper address).
    Returns: int
    """
    url = API_DELEGATORS_URL.format(valoper)
    data = fetch_json_with_retry(url)
    if data and 'pagination' in data and 'total' in data['pagination']:
        try:
            return int(data['pagination']['total'])
        except Exception as e:
            logging.warning(f"Error processing delegators for {valoper}: {e}")
    return 0

def get_validators_with_delegators():
    """
    Get a list of active (BONDED) validators with their delegator counts.
    Returns: list of dicts: {operator_address, moniker, delegators}
    """
    validators = get_all_validators_with_pagination()
    result = []
    for v in validators:
        valoper = v['operator_address']
        if v.get('status') != 'BOND_STATUS_BONDED':
            continue
        num_delegators = get_delegators_count(valoper)
        result.append({
            "operator_address": valoper,
            "moniker": v.get('description', {}).get('moniker', ''),
            "delegators": num_delegators,
        })
        logging.info(f"{valoper} ({v.get('description', {}).get('moniker', '')}): {num_delegators} delegators")
    return result 