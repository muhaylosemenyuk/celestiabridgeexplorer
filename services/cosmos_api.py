import requests
import time
import logging
from typing import Dict, List, Optional, Any
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
def get_accounts() -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of all Cosmos accounts.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request("/cosmos/auth/v1beta1/accounts", {})
get_accounts.is_pagination = True

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

def get_account(address: str) -> Optional[Dict[str, Any]]:
    """
    Returns details for a specific Cosmos account by address.

    Parameters:
      - address (str): The bech32 account address.

    Response:
      - account (dict): Account object with fields like 'address', 'pub_key', 'account_number', etc.
    """
    return make_cosmos_request(f"/cosmos/auth/v1beta1/accounts/{address}")

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

def get_block(height: int) -> Optional[Dict[str, Any]]:
    """
    Returns block data for a specific block height.

    Parameters:
      - height (int): Block height.

    Response:
      - block (dict): Block data including 'header', 'data', 'evidence', 'last_commit', etc.
    """
    return make_cosmos_request(f"/cosmos/base/tendermint/v1beta1/blocks/{height}")

def get_latest_validatorset() -> Optional[Dict[str, Any]]:
    """GET /cosmos/base/tendermint/v1beta1/validatorsets/latest"""
    return make_cosmos_request("/cosmos/base/tendermint/v1beta1/validatorsets/latest")
get_latest_validatorset.is_pagination = True

def get_validatorset(height: int) -> Optional[Dict[str, Any]]:
    """GET /cosmos/base/tendermint/v1beta1/validatorsets/{height}"""
    return make_cosmos_request(f"/cosmos/base/tendermint/v1beta1/validatorsets/{height}")
get_validatorset.is_pagination = True

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

def get_distribution_params() -> Optional[Dict[str, Any]]:
    """GET /cosmos/distribution/v1beta1/params"""
    return make_cosmos_request("/cosmos/distribution/v1beta1/params")

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

# --- Evidence Module ---
def get_evidence() -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of evidence.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request("/cosmos/evidence/v1beta1/evidence", {})
get_evidence.is_pagination = True

def get_evidence_by_hash(hash: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/evidence/v1beta1/evidence/{hash}"""
    return make_cosmos_request(f"/cosmos/evidence/v1beta1/evidence/{hash}")

# --- Governance Module v1beta1 ---
def get_gov_params_v1beta1(params_type: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1beta1/params/{params_type}"""
    return make_cosmos_request(f"/cosmos/gov/v1beta1/params/{params_type}")

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

def get_proposal_deposits_v1beta1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of deposits for a specific proposal (v1beta1).
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/gov/v1beta1/proposals/{proposal_id}/deposits", {})
get_proposal_deposits_v1beta1.is_pagination = True

def get_proposal_deposit_v1beta1(proposal_id: int, depositor: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1beta1/proposals/{proposal_id}/deposits/{depositor}"""
    return make_cosmos_request(f"/cosmos/gov/v1beta1/proposals/{proposal_id}/deposits/{depositor}")

def get_proposal_tally_v1beta1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1beta1/proposals/{proposal_id}/tally"""
    return make_cosmos_request(f"/cosmos/gov/v1beta1/proposals/{proposal_id}/tally")

def get_proposal_votes_v1beta1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of votes for a specific proposal (v1beta1).
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/gov/v1beta1/proposals/{proposal_id}/votes", {})
get_proposal_votes_v1beta1.is_pagination = True

def get_proposal_vote_v1beta1(proposal_id: int, voter: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1beta1/proposals/{proposal_id}/votes/{voter}"""
    return make_cosmos_request(f"/cosmos/gov/v1beta1/proposals/{proposal_id}/votes/{voter}")

# --- Governance Module v1 ---
def get_constitution() -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/constitution"""
    return make_cosmos_request("/cosmos/gov/v1/constitution")

def get_gov_params_v1() -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/params"""
    return make_cosmos_request("/cosmos/gov/v1/params")

def get_gov_params_by_msg_url_v1(msg_url: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/params/{msg_url}"""
    return make_cosmos_request(f"/cosmos/gov/v1/params/{msg_url}")

def get_proposals_v1(status: Optional[str] = None, voter: Optional[str] = None, depositor: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of governance proposals (v1).
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    params = {}
    if status:
        params["status"] = status
    if voter:
        params["voter"] = voter
    if depositor:
        params["depositor"] = depositor
    return make_cosmos_request("/cosmos/gov/v1/proposals", params)
get_proposals_v1.is_pagination = True

def get_proposal_v1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/proposals/{proposal_id}"""
    return make_cosmos_request(f"/cosmos/gov/v1/proposals/{proposal_id}")

def get_proposal_deposits_v1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of deposits for a specific proposal (v1).
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/gov/v1/proposals/{proposal_id}/deposits", {})
get_proposal_deposits_v1.is_pagination = True

def get_proposal_deposit_v1(proposal_id: int, depositor: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/proposals/{proposal_id}/deposits/{depositor}"""
    return make_cosmos_request(f"/cosmos/gov/v1/proposals/{proposal_id}/deposits/{depositor}")

def get_proposal_tally_v1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/proposals/{proposal_id}/tally"""
    return make_cosmos_request(f"/cosmos/gov/v1/proposals/{proposal_id}/tally")

def get_proposal_vote_options_v1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/proposals/{proposal_id}/vote_options"""
    return make_cosmos_request(f"/cosmos/gov/v1/proposals/{proposal_id}/vote_options")

def get_proposal_votes_v1(proposal_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of votes for a specific proposal (v1).
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/gov/v1/proposals/{proposal_id}/votes", {})
get_proposal_votes_v1.is_pagination = True

def get_proposal_vote_v1(proposal_id: int, voter: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/gov/v1/proposals/{proposal_id}/votes/{voter}"""
    return make_cosmos_request(f"/cosmos/gov/v1/proposals/{proposal_id}/votes/{voter}")

# --- Mint Module ---
def get_annual_provisions() -> Optional[Dict[str, Any]]:
    """GET /cosmos/mint/v1beta1/annual_provisions"""
    return make_cosmos_request("/cosmos/mint/v1beta1/annual_provisions")

def get_mint_params() -> Optional[Dict[str, Any]]:
    """GET /cosmos/mint/v1beta1/params"""
    return make_cosmos_request("/cosmos/mint/v1beta1/params")

# --- Slashing Module ---
def get_slashing_params() -> Optional[Dict[str, Any]]:
    """GET /cosmos/slashing/v1beta1/params"""
    return make_cosmos_request("/cosmos/slashing/v1beta1/params")

def get_signing_infos() -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of signing infos.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request("/cosmos/slashing/v1beta1/signing_infos", {})
get_signing_infos.is_pagination = True

def get_signing_info(cons_address: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/slashing/v1beta1/signing_infos/{cons_address}"""
    return make_cosmos_request(f"/cosmos/slashing/v1beta1/signing_infos/{cons_address}")

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

def get_unbonding_delegations(delegator_addr: str, validator: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of unbonding delegations for a specific delegator.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    params = {}
    if validator:
        params["validator_addr"] = validator
    return make_cosmos_request(f"/cosmos/staking/v1beta1/delegators/{delegator_addr}/unbonding_delegations", params)
get_unbonding_delegations.is_pagination = True

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

def get_historical_info(height: int) -> Optional[Dict[str, Any]]:
    """
    Returns historical staking information at a specific block height.

    Parameters:
      - height (int): Block height.

    Response:
      - hist (dict): Historical info object with staking data at the given height.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/historical_info/{height}")

def get_staking_params() -> Optional[Dict[str, Any]]:
    """
    Returns the current staking module parameters.

    Response:
      - params (dict): Staking parameters (e.g. unbonding time, max validators, etc).
    """
    return make_cosmos_request("/cosmos/staking/v1beta1/params")

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

def get_validator_unbonding_delegation(validator_addr: str, delegator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns details of a specific unbonding delegation from a delegator to a validator.

    Parameters:
      - validator_addr (str): The bech32 operator address of the validator.
      - delegator_addr (str): The bech32 address of the delegator.

    Response:
      - unbond (dict): Unbonding delegation object.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/validators/{validator_addr}/delegations/{delegator_addr}/unbonding_delegation")

def get_validator_unbonding_delegations(validator_addr: str) -> Optional[Dict[str, Any]]:
    """
    Returns a paginated list of unbonding delegations for a specific validator.
    This endpoint returns paginated results. Use universal pagination for full data aggregation.
    """
    return make_cosmos_request(f"/cosmos/staking/v1beta1/validators/{validator_addr}/unbonding_delegations", {})
get_validator_unbonding_delegations.is_pagination = True

# --- Upgrade Module ---
def get_applied_plan(name: str) -> Optional[Dict[str, Any]]:
    """GET /cosmos/upgrade/v1beta1/applied_plan/{name}"""
    return make_cosmos_request(f"/cosmos/upgrade/v1beta1/applied_plan/{name}")

def get_upgrade_authority() -> Optional[Dict[str, Any]]:
    """GET /cosmos/upgrade/v1beta1/authority"""
    return make_cosmos_request("/cosmos/upgrade/v1beta1/authority")

def get_current_plan() -> Optional[Dict[str, Any]]:
    """GET /cosmos/upgrade/v1beta1/current_plan"""
    return make_cosmos_request("/cosmos/upgrade/v1beta1/current_plan")

def get_module_versions() -> Optional[Dict[str, Any]]:
    """GET /cosmos/upgrade/v1beta1/module_versions"""
    return make_cosmos_request("/cosmos/upgrade/v1beta1/module_versions")

def get_upgraded_consensus_state(last_height: int) -> Optional[Dict[str, Any]]:
    """GET /cosmos/upgrade/v1beta1/upgraded_consensus_state/{last_height}"""
    return make_cosmos_request(f"/cosmos/upgrade/v1beta1/upgraded_consensus_state/{last_height}")
