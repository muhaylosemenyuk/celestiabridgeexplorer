import logging
from typing import Dict, Any, List

class ResponseFormatter:
    def __init__(self, llm_client):
        self.llm = llm_client
        self.logger = logging.getLogger("celestia_mcp.response_formatter")

    async def format(self, plan: Dict[str, Any], api_results: Dict[str, Any], locale: str = None, chat_history: List[Dict[str, Any]] = None) -> str:
        chat_history = chat_history or []
        prompt = f"""
You are an AI assistant for CelestiaBridge. Format the response for the user based on the following:
Always answer in the language of the user's query.

- Write in a simple, human-friendly style.
- Use short sentences and clear explanations.
- Separate ideas into paragraphs.
- Prefer bullet lists for facts, steps, or options.
- Avoid excessive formality or technical jargon unless the user requests it.
- If you see a balance in 'utia' (micro-TIA), and the value is greater than 1,000,000, always convert it to TIA (TIA = utia / 1_000_000) and show the result in TIA for user-friendly display.
- **If the API results are empty or missing, do NOT invent or hallucinate data. Clearly tell the user that the information is unavailable or not found.**

User chat context (last 5): {chat_history[-5:]}

User intent and plan:
{plan}

API results:
{api_results}

Generate a clear, helpful, and concise answer in the user's language if possible.
Return only the final response text.
"""
        response = await self.llm(prompt)
        self.logger.info(f"LLM formatted response: {response}")
        return response.strip()
