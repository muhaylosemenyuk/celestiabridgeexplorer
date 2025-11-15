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

    async def route(self, user_message: str, locale: str = None, chat_history: List[Dict[str, Any]] = None, history_for_prompt: List[Dict[str, Any]] = None) -> Dict:
        """
        Forms a prompt for the LLM and returns a JSON plan:
        - intent
        - endpoints: [{name, parameters, reason}]
        - analysis_steps
        - confidence
        """
        chat_history = chat_history or []
        docs = self.registry.get_llm_docs()
        
        # Use provided history_for_prompt or compute from chat_history
        if history_for_prompt is None:
            history_for_prompt = chat_history[-5:]
        logger.info(f"LLM Router - History sent to prompt: {history_for_prompt}")
        
        prompt = f"""
You are an AI assistant for CelestiaBridge. Analyze the user query and decide whether to use API endpoints, answer directly, or provide CLI consultation.

QUERY TYPE DETECTION:
- If the user asks about general information about Celestia blockchain (what is Celestia, how it works, technology, features, etc.) → use DIRECT_ANSWER
- If the user asks about CLI commands, how to execute commands on nodes, node operations, command syntax, or wants to know what commands to run → use CLI_CONSULTATION
- If the user asks about specific metrics, data, validators, nodes, balances, delegations → use API ENDPOINTS
- If the user asks about current network status, validator performance, specific addresses → use API ENDPOINTS

RULES:
- IMPORTANT: Always answer in the English language
- Display amounts in TIA (never utia)
- For Cosmos endpoints: convert TIA→utia for filtering (TIA * 1_000_000)
- For local endpoints: use TIA directly
- CRITICAL: Use ONLY field names from the endpoint documentation below
- NEVER invent or guess field names - they must match exactly what's in the docs
- For sorting/grouping: use exact field names from TABLE SCHEMA sections
- Use appropriate limit: "top N"→N, "all"→1000, "recent"→20-50, general→10

ENDPOINT SELECTION:
- validators: for validator data
- nodes: for node/geographic data and decentralization analysis
- balances: for wallet balance data
- chain: for chain metrics
- metrics/aggregate: for performance metrics
- releases: for software releases
- delegations: for delegation data (use validator_address to filter by validator)
- Cosmos endpoints: for blockchain data

IMPORTANT PARAMETER MAPPING:
- When user provides a validator address (celestiavaloper...), use validator_address parameter
- When user provides a delegator address (celestia1...), use delegator_address parameter
- For delegations endpoint: use validator_address to get delegators of that validator
- For balances endpoint: use address parameter for specific wallet balance

METRICS ENDPOINT (metrics_aggregate) IMPORTANT NOTES:
- For is_sync metric: values are percentages from 0-100 (not 0-1)
- To find nodes with 100% synchronization: use min_max_value=100&max_max_value=100 (NOT min_value=1&max_value=1)
- Use post-aggregation filters for aggregated values:
  * min_max_value/max_max_value: Filter by maximum sync value (last known sync level)
  * min_avg_value/max_avg_value: Filter by average sync value over the period
- Pre-aggregation filters (min_value/max_value) filter raw metric records before grouping
- Post-aggregation filters (min_avg_value/max_avg_value/min_max_value/max_max_value) filter after aggregation
- Example: "bridges with 100% sync" → metric_name=is_sync&min_max_value=100&max_max_value=100

SEQUENTIAL REQUESTS:
- For complex queries requiring multiple steps, use sequential requests
- Set "sequential": true in JSON response
- Define "steps" array with ordered requests
- Use "depends_on" for step dependencies
- Use "use_results_from" for chaining data between steps
- Example: "Show balances of delegators to validator X"
  - Step 1: Get delegator addresses from delegations endpoint
  - Step 2: Use those addresses to query balances endpoint
- Use {{step1.field}} syntax to reference results from previous steps (double curly braces!)
- Extract specific fields using "extract_fields" array

Available endpoints:
{docs}

User query: {user_message}
Chat context (last 5): {history_for_prompt}

Examples:

Direct answer for general blockchain questions:
{{
  "intent": "answer general question about Celestia blockchain",
  "direct_answer": true,
  "analysis_steps": ["provide general information about Celestia"],
  "confidence": 0.95
}}

Basic endpoint:
{{
  "name": "chain",
  "parameters": {{}}
}}

Filtered query:
{{
  "name": "validators",
  "parameters": {{
    "operator_address": "celestiavaloper1example",
    "limit": 1,
    "order_by": "missed_blocks_counter",
    "order_direction": "desc"
  }}
}}

Aggregated query:
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

Sequential query (multi-step):
{{
  "intent": "get delegator balances for validator",
  "sequential": true,
  "steps": [
    {{
      "name": "delegations",
      "parameters": {{
        "validator_address": "celestiavaloper1example",
        "limit": 10
      }},
      "extract_fields": ["delegator_address"]
    }},
    {{
      "name": "balances",
      "parameters": {{
        "address": "{{{{step1.delegator_address}}}}"
      }},
      "depends_on": "step1"
    }}
  ]
}}

IMPORTANT: Return ONLY the JSON object, no explanations, no markdown, no additional text. Just the pure JSON:

For direct answers (general blockchain questions):
{{
  "intent": "answer general question about Celestia blockchain",
  "direct_answer": true,
  "analysis_steps": ["provide general information about Celestia"],
  "confidence": 0.95
}}

For CLI consultation (questions about CLI commands):
{{
  "intent": "provide CLI command consultation",
  "cli_consultation": true,
  "analysis_steps": ["provide CLI command guidance"],
  "confidence": 0.95
}}

For single requests:
{{
  "intent": "main goal",
  "endpoints": [{{ ... }}],
  "analysis_steps": ["step1", "step2"],
  "confidence": 0.95
}}

For sequential requests:
{{
  "intent": "main goal",
  "sequential": true,
  "steps": [{{ ... }}],
  "analysis_steps": ["step1", "step2"],
  "confidence": 0.95
}}
"""
        logger.info(f"LLM prompt length: {len(prompt)} chars, {len(prompt.encode('utf-8'))} bytes")
        llm_response = await self.llm(prompt)
        
        # Handle None response from LLM
        if llm_response is None:
            logger.error("LLM returned None response")
            return {
                "intent": "Error: LLM returned no response",
                "endpoints": [],
                "analysis_steps": ["LLM service unavailable"],
                "confidence": 0.0
            }
        
        cleaned_response = llm_response.strip()
        
        # Try to extract JSON from the response
        json_start = cleaned_response.find('{')
        json_end = cleaned_response.rfind('}') + 1
        
        if json_start != -1 and json_end > json_start:
            cleaned_response = cleaned_response[json_start:json_end]
        elif cleaned_response.startswith('```json'):
            cleaned_response = cleaned_response[7:]  
        elif cleaned_response.endswith('```'):
            cleaned_response = cleaned_response[:-3]
        
        cleaned_response = cleaned_response.strip()
        
        try:
            plan = json.loads(cleaned_response)
        except Exception as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            logger.error(f"Cleaned response: {cleaned_response}")
            
            # Try to fix common JSON issues
            try:
                # Remove trailing commas
                cleaned_response = cleaned_response.replace(',}', '}').replace(',]', ']')
                # Remove any trailing text after the last }
                last_brace = cleaned_response.rfind('}')
                if last_brace != -1:
                    cleaned_response = cleaned_response[:last_brace + 1]
                
                plan = json.loads(cleaned_response)
                logger.info("Successfully parsed JSON after cleaning")
            except Exception as e2:
                logger.error(f"Still failed to parse after cleaning: {e2}")
                # Try to create a fallback plan
                plan = {
                    "intent": "Error: Could not parse LLM response",
                    "endpoints": [],
                    "analysis_steps": ["Failed to parse LLM response as JSON"],
                    "confidence": 0.0,
                    "error": str(e),
                    "raw_response": llm_response[:500] + "..." if len(llm_response) > 500 else llm_response
                }
        return plan
