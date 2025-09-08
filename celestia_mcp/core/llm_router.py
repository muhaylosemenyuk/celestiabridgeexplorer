import json
from typing import List, Dict, Any
import logging

logger = logging.getLogger("celestia_mcp.llm_router")

class LLMRouter:
    def __init__(self, api_registry, llm_client):
        self.registry = api_registry
        self.llm = llm_client

    async def route(self, user_message: str, locale: str = None, chat_history: List[Dict[str, Any]] = None) -> Dict:
        """
        Forms a prompt for the LLM and returns a JSON plan:
        - intent
        - endpoints: [{name, parameters, reason}]
        - analysis_steps
        - confidence
        """
        chat_history = chat_history or []
        docs = self.registry.get_llm_docs()
        prompt = f"""
You are an AI assistant for CelestiaBridge. Analyze the user query and select API endpoints to call.

- Always answer in the language of the user's query.
- All amounts in API results are in utia (micro-TIA).
- If the user query mentions TIA, always convert TIA to utia for filtering and utia to TIA for display. 1 TIA = 1,000,000 utia.
- Example: If the user asks for delegators with more than 1,000,000 TIA, filter for amount > 1_000_000_000_000 utia.
- For Cosmos REST API endpoints with is_pagination = True, use 'pagination_aggregate' if the query requires aggregation over all data (e.g., top N, sum, unique, etc.).
- For local endpoints (like /chain, /nodes) or non-paginated endpoints, always use a standard API call (do not use pagination_aggregate).

Available endpoints:
{docs}

User query: {user_message}
Chat context (last 5): {chat_history[-5:]}

Example for standard local endpoint:
{{
  "name": "chain",
  "parameters": {{}}
}}

Example for paginated Cosmos endpoint:
{{
  "name": "get_validator_delegations",
  "pagination_aggregate": {{
    "endpoint": "/cosmos/staking/v1beta1/validators/{{validator_addr}}/delegations",
    "params": {{"validator_addr": "..."}},
    "item_path": "delegation_responses",
    "aggregate": "top",
    "aggregate_field": "balance.amount",
    "top_n": 5,
    "sort_desc": true,
    "filter": {{
      "field": "balance.amount",
      "operator": ">",
      "value": 1000000000000
    }}
  }}
}}

Example for chaining endpoints:
[
  {{
    "name": "get_latest_block_height",
    "parameters": {{}}
  }},
  {{
    "name": "get_block",
    "parameters": {{
      "height": "from_get_latest_block_height"
    }}
  }}
]

Return JSON:
{{
  "intent": "main goal",
  "endpoints": [{{ ... }}],
  "analysis_steps": ["step1", "step2"],
  "confidence": 0.95
}}
"""
        logger.info(f"LLM prompt length: {len(prompt)} chars, {len(prompt.encode('utf-8'))} bytes")
        llm_response = await self.llm(prompt)
        logger.info(f"LLM response:\n{llm_response}")
        try:
            plan = json.loads(llm_response)
        except Exception:
            plan = {"error": "LLM returned invalid JSON", "raw": llm_response}
        return plan
