import logging
import re
from typing import Dict, Any, List
from celestia_mcp.core.llm_router import get_llm_client

class ResponseFormatter:
    def __init__(self, llm_client=None):
        self.llm = llm_client or get_llm_client()
        self.logger = logging.getLogger("celestia_mcp.response_formatter")

    def clean_markdown_formatting(self, text: str) -> str:
        """
        Removes double asterisks ** from AI response text for web chat
        """
        if not text:
            return text
        
        # Remove all double asterisks **
        cleaned_text = re.sub(r'\*\*', '', text)
        
        return cleaned_text

    async def format(self, plan: Dict[str, Any], api_results: Dict[str, Any], user_query: str = None, locale: str = None, chat_history: List[Dict[str, Any]] = None) -> str:
        chat_history = chat_history or []
      
        # Logging history that is passed to prompt
        history_for_prompt = chat_history[-5:]
        
        # Check if this is a direct answer request
        if plan.get("direct_answer"):
            prompt = f"""
You are an AI assistant for CelestiaBridge. Answer the user's question about Celestia blockchain using your knowledge.
Always answer in the language of the user's query from User query.

- Write in a simple, human-friendly style.
- Use short sentences and clear explanations.
- Separate ideas into paragraphs.
- Prefer bullet lists for facts, steps, or options.
- Avoid excessive formality or technical jargon unless the user requests it.
- IMPORTANT: Always answer in the language of the User query
- Provide comprehensive information about Celestia blockchain based on your knowledge
- Include key features, technology, and benefits of Celestia

User query: {user_query or "No user query provided"}

User chat context (last 5): {history_for_prompt}

User intent and plan:
{plan}

Generate a clear, helpful, and comprehensive answer about Celestia blockchain in the user's language.
Return only the final response text.
"""
        else:
            prompt = f"""
You are an AI assistant for CelestiaBridge. Format the response for the user based on the following:
Always answer in the language of the user's query from User query.

- Write in a simple, human-friendly style.
- Use short sentences and clear explanations.
- Separate ideas into paragraphs.
- Prefer bullet lists for facts, steps, or options.
- Avoid excessive formality or technical jargon unless the user requests it.
- IMPORTANT: Always display amounts in TIA, never in utia. Convert utia to TIA for display (TIA = utia / 1_000_000).
- For all balance-related information, show values in TIA format for better readability.
- **If the API results are empty or missing, do NOT invent or hallucinate data. Clearly tell the user that the information is unavailable or not found.**
- IMPORTANT: Always answer in the language of the User query

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

User query: {user_query or "No user query provided"}

User chat context (last 5): {history_for_prompt}

User intent and plan:
{plan}

API results:
{api_results}

Generate a clear, helpful, and concise answer in the user's language if possible.
Return only the final response text.
"""
        self.logger.info(f"Response Formatter prompt:\n{prompt}")
        response = await self.llm(prompt)
        cleaned_response = self.clean_markdown_formatting(response.strip())
        self.logger.info(f"LLM formatted response: {cleaned_response}")
        
        return cleaned_response
