import json
from typing import List, Dict, Any
import logging
from config import LLM_PROVIDER
from celestia_mcp.grok_llm_client import llm_client as grok_llm_client
from celestia_mcp.gemini_llm_client import GeminiLLMClient

logger = logging.getLogger("celestia_mcp.llm_router")

def get_llm_client():
    if LLM_PROVIDER.lower() == "gemini":
        logger.info("LLM provider: GEMINI")
        return GeminiLLMClient()
    logger.info("LLM provider: GROK")
    return grok_llm_client

class LLMRouter:
    def __init__(self, api_registry, llm_client=None):
        self.registry = api_registry
        if llm_client is None:
            self.llm = get_llm_client()
            logger.info(f"LLMRouter: Using auto-selected LLM client: {type(self.llm).__name__}")
        else:
            self.llm = llm_client
            logger.info(f"LLMRouter: Using explicitly provided LLM client: {type(self.llm).__name__}")

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
        
        # Logging history that is passed to prompt
        history_for_prompt = chat_history[-5:]
        logger.info(f"LLM Router - History sent to prompt: {history_for_prompt}")
        
        prompt = f"""
You are an AI assistant for CelestiaBridge. Analyze the user query and select API endpoints to call.

RULES:
- Always answer in the language of the user's query
- Display amounts in TIA (never utia)
- For Cosmos endpoints: convert TIA→utia for filtering (TIA * 1_000_000)
- For local endpoints: use TIA directly
- CRITICAL: Use ONLY field names from the endpoint documentation below
- NEVER invent or guess field names - they must match exactly what's in the docs
- For sorting/grouping: use exact field names from TABLE SCHEMA sections

ENDPOINT SELECTION:
- nodes: for node/geographic data
- balances: for wallet balance data (use aggregations parameter for complex queries)
- chain: for chain metrics
- metrics: for performance metrics
- releases: for software releases
- Cosmos endpoints: for blockchain data

Available endpoints:
{docs}

User query: {user_message}
Chat context (last 5): {history_for_prompt}

Examples:

Standard endpoint:
{{
  "name": "chain",
  "parameters": {{}}
}}

Balance query with aggregation (use exact field names from docs):
{{
  "name": "balances",
  "parameters": {{
    "min_balance": 1000,
    "order_by": "balance_tia",
    "order_direction": "desc",
    "aggregations": "[{{\"type\": \"count\"}}]",
    "return_format": "aggregated"
  }}
}}

Node query with grouping (use exact field names from docs):
{{
  "name": "nodes",
  "parameters": {{
    "group_by": "region",
    "aggregations": "[{{\"type\": \"count\"}}]",
    "return_format": "aggregated"
  }}
}}

Cosmos endpoint with pagination:
{{
  "name": "get_validator_delegations",
  "pagination_aggregate": {{
    "endpoint": "/cosmos/staking/v1beta1/validators/{{validator_addr}}/delegations",
    "params": {{"validator_addr": "..."}},
    "item_path": "delegation_responses",
    "aggregate": "top",
    "aggregate_field": "balance.amount",
    "top_n": 5
  }}
}}

Chaining endpoints (use result from first call in second):
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
        
        cleaned_response = llm_response.strip()
        if cleaned_response.startswith('```json'):
            cleaned_response = cleaned_response[7:]  
        if cleaned_response.endswith('```'):
            cleaned_response = cleaned_response[:-3]
        cleaned_response = cleaned_response.strip()
        
        try:
            plan = json.loads(cleaned_response)
        except Exception as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            logger.error(f"Cleaned response: {cleaned_response}")
            plan = {"error": "LLM returned invalid JSON", "raw": llm_response}
        return plan
