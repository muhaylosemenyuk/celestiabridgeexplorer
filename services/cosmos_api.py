import requests
import time
import logging
from typing import Dict, Optional, Any
from config import COSMOS_API_BASE_URL

RETRIES = 3
DELAY = 5

def make_cosmos_request(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Universal function for requests to Cosmos API.
    
    Args:
        endpoint: Endpoint without base URL (e.g.: "/cosmos/auth/v1beta1/accounts")
        params: Request parameters
    
    Returns:
        Dict with response or None on error
    """
    url = f"{COSMOS_API_BASE_URL}{endpoint}"
    
    for attempt in range(RETRIES):
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.warning(f"Error fetching {url}: {e}")
            if attempt < RETRIES - 1:
                logging.info(f"Retrying in {DELAY} seconds...")
                time.sleep(DELAY)
            else:
                logging.error(f"Max retries reached for {url}")
                return None

# --- Auth Module ---
def get_wallet_addresses_count() -> Optional[int]:
    """GET /cosmos/auth/v1beta1/accounts - returns the total number of wallet addresses"""
    data = make_cosmos_request("/cosmos/auth/v1beta1/accounts", {
        "pagination.limit": 1,
        "pagination.count_total": "true"
    })
    
    if data and "pagination" in data and "total" in data["pagination"]:
        try:
            return int(data["pagination"]["total"])
        except (ValueError, TypeError) as e:
            logging.error(f"Error parsing total accounts count: {e}")
            return None
    
    return None

# --- Bank Module ---
def get_balances(address: str) -> Optional[Dict[str, Any]]:
    """
    Returns all token balances for a given account address.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/bank/v1beta1/balances/{address}", {})
get_balances.is_pagination = True

# --- Base/Tendermint Module ---
def get_latest_block_height() -> Optional[int]:
    """
    Returns the height of the latest block in the blockchain.

    Response:
      - height (int): Block height as integer.
    """
    data = make_cosmos_request("/cosmos/base/tendermint/v1beta1/blocks/latest")
    if data and "block" in data and "header" in data["block"]:
        try:
            return int(data["block"]["header"].get("height"))
        except (ValueError, TypeError):
            return None
    return None


# --- Distribution Module ---
def get_community_pool() -> Optional[Dict[str, Any]]:
    """
    Returns the current community pool balance.

    Response:
      - pool (list): List of coins in the community pool, each with 'denom' and 'amount'.
    """
    return make_cosmos_request("/cosmos/distribution/v1beta1/community_pool")

def get_delegator_rewards(delegator_address: str) -> Optional[Dict[str, Any]]:
    """
    Returns all rewards earned by a specific delegator from all validators.

    Parameters:
      - delegator_address (str): The bech32 address of the delegator.

    Response:
      - rewards (list): List of reward objects per validator.
      - total (list): Total rewards across all validators.
    """
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/delegators/{delegator_address}/rewards")

def get_delegator_validator_rewards(delegator_address: str, validator_address: str) -> Optional[Dict[str, Any]]:
    """
    Returns rewards earned by a delegator from a specific validator.

    Parameters:
      - delegator_address (str): The bech32 address of the delegator.
      - validator_address (str): The bech32 address of the validator.

    Response:
      - rewards (list): List of reward objects for the specified validator.
    """
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/delegators/{delegator_address}/rewards/{validator_address}")

def get_delegator_validators(delegator_address: str) -> Optional[Dict[str, Any]]:
    """
    Returns a list of all validators a delegator is bonded to.

    Parameters:
      - delegator_address (str): The bech32 address of the delegator.

    Response:
      - validators (list): List of validator operator addresses.
    """
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/delegators/{delegator_address}/validators")

def get_delegator_withdraw_address(delegator_address: str) -> Optional[Dict[str, Any]]:
    """
    Returns the withdraw address for a specific delegator (where rewards are sent).

    Parameters:
      - delegator_address (str): The bech32 address of the delegator.

    Response:
      - withdraw_address (str): The bech32 withdraw address.
    """
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/delegators/{delegator_address}/withdraw_address")


def get_validator_info(validator_address: str) -> Optional[Dict[str, Any]]:
    """
    Returns distribution information for a specific validator.

    Parameters:
      - validator_address (str): The bech32 address of the validator.

    Response:
      - validator (dict): Validator distribution info (commission, outstanding rewards, etc).
    """
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/validators/{validator_address}")

def get_validator_commission(validator_address: str) -> Optional[Dict[str, Any]]:
    """
    Returns the commission earned by a specific validator.

    Parameters:
      - validator_address (str): The bech32 address of the validator.

    Response:
      - commission (dict): Commission object with details about validator's commission.
    """
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/validators/{validator_address}/commission")

def get_validator_outstanding_rewards(validator_address: str) -> Optional[Dict[str, Any]]:
    """
    Returns the outstanding (unwithdrawn) rewards for a specific validator.

    Parameters:
      - validator_address (str): The bech32 address of the validator.

    Response:
      - rewards (dict): Outstanding rewards object.
    """
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/validators/{validator_address}/outstanding_rewards")

def get_validator_slashes(validator_address: str, start_height: Optional[int] = None, end_height: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Returns all slashing events for a specific validator within an optional height range.

    Parameters:
      - validator_address (str): The bech32 address of the validator.
      - start_height (int, optional): Start block height (inclusive).
      - end_height (int, optional): End block height (inclusive).

    Response:
      - slashes (list): List of slashing event objects.
    """
    params = {}
    if start_height:
        params["starting_height"] = start_height
    if end_height:
        params["ending_height"] = end_height
    return make_cosmos_request(f"/cosmos/distribution/v1beta1/validators/{validator_address}/slashes", params)


# --- Governance Module v1beta1 ---
def get_proposals_v1beta1(status: Optional[str] = None, voter: Optional[str] = None, depositor: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of governance proposals (v1beta1).
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    params = {}
    if status:
        params["proposal_status"] = status
    if voter:
        params["voter"] = voter
    if depositor:
        params["depositor"] = depositor
    return make_cosmos_request("/cosmos/gov/v1beta1/proposals", params)
get_proposals_v1beta1.is_pagination = True

def get_proposal_v1beta1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns details for a specific governance proposal (v1beta1).

    Parameters:
      - proposal_id (int): The ID of the proposal.

    Response:
      - proposal (dict): Proposal object with fields like 'proposal_id', 'content', 'status', etc.
    """
    return make_cosmos_request(f"/cosmos/gov/v1beta1/proposals/{proposal_id}")


# --- Staking Module ---
def get_delegations(delegator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of delegations for a specific delegator.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/delegations/{delegator_addr}", {})
get_delegations.is_pagination = True

def get_redelegations(delegator_addr: str, src_validator: Optional[str] = None, dst_validator: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of redelegations for a specific delegator.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    params = {}
    if src_validator:
        params["src_validator_addr"] = src_validator
    if dst_validator:
        params["dst_validator_addr"] = dst_validator
    return make_cosmos_request(f"/cosmos/staking/v1beta1/delegators/{delegator_addr}/redelegations", params)
get_redelegations.is_pagination = True


def get_delegator_validators(delegator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of validators for a specific delegator.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/delegators/{delegator_addr}/validators", {})
get_delegator_validators.is_pagination = True

def get_delegator_validator(delegator_addr: str, validator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns details for a specific validator delegated to by a delegator.

    Parameters:
      - delegator_addr (str): The bech32 address of the delegator.
      - validator_addr (str): The bech32 address of the validator.

    Response:
      - validator (dict): Validator object.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/delegators/{delegator_addr}/validators/{validator_addr}")

def get_staking_pool() -> Optional[Dict[str, Any]]:
    """
    Returns the current staking pool information.

    Response:
      - pool (dict): Staking pool object with fields like 'bonded_tokens', 'not_bonded_tokens', etc.
    """
    return make_cosmos_request("/cosmos/staking/v1beta1/pool")

def get_validators(status: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of validators.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    params = {}
    if status:
        params["status"] = status
    return make_cosmos_request("/cosmos/staking/v1beta1/validators", params)
get_validators.is_pagination = True

def get_validator(validator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns details for a specific validator by operator address.

    Parameters:
      - validator_addr (str): The bech32 operator address of the validator.

    Response:
      - validator (dict): Validator object with fields like 'operator_address', 'description', 'jailed', 'status', etc.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/validators/{validator_addr}")

def get_validator_delegations(validator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of delegations to a specific validator.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/validators/{validator_addr}/delegations", {})
get_validator_delegations.is_pagination = True

def get_validator_delegation(validator_addr: str, delegator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns details of a specific delegation from a delegator to a validator.

    Parameters:
      - validator_addr (str): The bech32 operator address of the validator.
      - delegator_addr (str): The bech32 address of the delegator.

    Response:
      - delegation_response (dict): Delegation object.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/validators/{validator_addr}/delegations/{delegator_addr}")

