import logging
import os
import re
from typing import Dict, Any, List
from celestia_mcp.core.llm_router import get_llm_client

# Common prompt rules used across all formatter prompts
COMMON_PROMPT_RULES = """
- Write in a simple, human-friendly style.
- Use short sentences and clear explanations.
- IMPORTANT: Always answer in the English language
- Generate a clear, helpful, and concise answer
"""

class ResponseFormatter:
    def __init__(self, llm_client=None):
        self.llm = llm_client or get_llm_client()
        self.logger = logging.getLogger("celestia_mcp.response_formatter")
        self.cli_docs = self._load_cli_docs()
    
    def _load_cli_docs(self) -> str:
        """Load CLI commands documentation from file."""
        cli_docs_path = os.path.join(os.path.dirname(__file__), "..", "docs", "cli_commands.md")
        try:
            if os.path.exists(cli_docs_path):
                with open(cli_docs_path, "r", encoding="utf-8") as f:
                    docs = f.read()
                    self.logger.info(f"Loaded CLI documentation for ResponseFormatter")
                    return docs
            else:
                self.logger.warning(f"CLI documentation file not found at {cli_docs_path}")
                return ""
        except Exception as e:
            self.logger.error(f"Failed to load CLI documentation: {e}")
            return ""

    def clean_markdown_formatting(self, text: str) -> str:
        """
        Removes double asterisks ** from AI response text for web chat
        """
        if not text:
            return text
        
        # Remove all double asterisks **
        cleaned_text = re.sub(r'\*\*', '', text)
        
        return cleaned_text

    async def format(self, plan: Dict[str, Any], api_results: Dict[str, Any], user_query: str = None, locale: str = None, chat_history: List[Dict[str, Any]] = None, history_for_prompt: List[Dict[str, Any]] = None) -> str:
        # Use provided history_for_prompt or compute from chat_history
        if history_for_prompt is None:
            chat_history = chat_history or []
            history_for_prompt = chat_history[-5:]
        
        # Common context used in all prompts
        common_context = f"""
User query: {user_query or "No user query provided"}

User chat context (last 5): {history_for_prompt}

User intent and plan:
{plan}
"""
        
        # Check if this is a CLI consultation request
        if plan.get("cli_consultation"):
            prompt = f"""
You are an AI assistant for CelestiaBridge. Provide CLI command consultation based on the user's question.
{COMMON_PROMPT_RULES}
- Provide exact CLI commands that the user can copy and execute.
- Explain what each command does and when to use it.
- Include examples with placeholders that the user should replace (e.g., <ADDRESS>, <NODE_URI>).
- **IMPORTANT: Always prioritize MAINNET examples in CLI commands. Use mainnet endpoints and chain-id (celestia) by default.**
- Mainnet RPC: https://rpc.celestia.pops.one:443
- Mainnet gRPC: public-celestia-grpc.numia.xyz:9090
- Mainnet chain-id: celestia
- Only mention testnet (mocha-4) if the user specifically asks for it.
- If the user asks about a specific operation, provide the relevant command(s) from the CLI documentation.
- Format commands in code blocks for clarity
- Provide context about when and why to use each command

CLI Commands Documentation:
{self.cli_docs}
{common_context}
Return only the final response text.
"""
        # Check if this is a direct answer request
        elif plan.get("direct_answer"):
            prompt = f"""
You are an AI assistant for CelestiaBridge. Answer the user's question about Celestia blockchain using your knowledge.
{COMMON_PROMPT_RULES}
- Separate ideas into paragraphs.
- Prefer bullet lists for facts, steps, or options.
- Avoid excessive formality or technical jargon unless the user requests it.
- Provide comprehensive information about Celestia blockchain based on your knowledge
- Include key features, technology, and benefits of Celestia
{common_context}
Return only the final response text.
"""
        else:
            prompt = f"""
You are an AI assistant for CelestiaBridge. Format the response for the user based on the following:
{COMMON_PROMPT_RULES}
- Separate ideas into paragraphs.
- Prefer bullet lists for facts, steps, or options.
- Avoid excessive formality or technical jargon unless the user requests it.
- IMPORTANT: Always display amounts in TIA, never in utia. Convert utia to TIA for display (TIA = utia / 1_000_000).
- For all balance-related information, show values in TIA format for better readability.
- **If the API results are empty or missing, do NOT invent or hallucinate data. Clearly tell the user that the information is unavailable or not found.**

DECENTRALIZATION ANALYSIS GUIDELINES:
- When analyzing bridge nodes data, pay special attention to decentralization metrics:
  * If provider_hetzner=true: Warn about Hetzner concentration and recommend diversifying providers
  * If city_over_limit=true: Indicate poor city-level decentralization
  * If country_over_limit=true: Indicate poor country-level decentralization  
  * If continent_over_limit=true: Indicate poor continent-level decentralization
  * If provider_over_limit=true: Indicate poor provider-level decentralization
- Provide actionable recommendations for improving decentralization
- Highlight risks when concentration is too high in any dimension
- Suggest specific improvements based on the data patterns observed
{common_context}
API results:
{api_results}

Return only the final response text.
"""
        self.logger.info(f"Response Formatter prompt:\n{prompt}")
        response = await self.llm(prompt)
        cleaned_response = self.clean_markdown_formatting(response.strip())
        self.logger.info(f"LLM formatted response: {cleaned_response}")
        
        return cleaned_response
