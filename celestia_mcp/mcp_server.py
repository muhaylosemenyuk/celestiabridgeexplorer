from fastmcp import FastMCP
from celestia_mcp.core.api_registry import APIRegistry
from celestia_mcp.core.llm_router import LLMRouter
from celestia_mcp.core.api_executor import APIExecutor
from celestia_mcp.core.response_formatter import ResponseFormatter
import logging

logger = logging.getLogger("celestia_mcp.mcp_server")

class CelestiaMCP(FastMCP):
    def __init__(self, llm_client, local_api_url="http://localhost:8000"):
        super().__init__("CelestiaBridge MCP")
        self.registry = APIRegistry()
        self.llm_router = LLMRouter(self.registry, llm_client)
        self.api_executor = APIExecutor(self.registry, local_api_url)
        self.response_formatter = ResponseFormatter(llm_client)
        self.chat_context = {}  # user_id -> list of dicts

    @property
    def tools(self):
        return {
            "consult_celestia": self.consult_celestia
        }

    async def consult_celestia(self, user_message: str, user_id: str = "default", locale: str = None):
        """
        Universal AI assistant for CelestiaBridge. Accepts queries in any language and always answers in the language of the user's query.
        """
        history = self.chat_context.get(user_id, [])
        logger.info(f"User {user_id} message: {user_message}")
        logger.info(f"Chat history for user {user_id}: {len(history)} messages")
        if history:
            logger.info(f"Last 3 messages: {history[-3:]}")
        
        # Compute history_for_prompt once (used by both router and formatter)
        history_for_prompt = history[-5:]
        
        # If locale is not specified, use automatic detection through prompt
        plan = await self.llm_router.route(user_message, locale, history, history_for_prompt)
        
        # Check if this is a CLI consultation request
        if plan.get("cli_consultation"):
            logger.info(f"LLM selected CLI consultation for command-related question")
            api_results = {}  # Empty results for CLI consultations
            response = await self.response_formatter.format(plan, api_results, user_message, locale, history, history_for_prompt)
        # Check if this is a direct answer request
        elif plan.get("direct_answer"):
            logger.info(f"LLM selected direct answer for general blockchain question")
            api_results = {}  # Empty results for direct answers
            response = await self.response_formatter.format(plan, api_results, user_message, locale, history, history_for_prompt)
        # Check if this is a sequential request
        elif plan.get("sequential"):
            logger.info(f"LLM selected sequential request with {len(plan.get('steps', []))} steps")
            api_results = await self.api_executor.execute(plan)
            response = await self.response_formatter.format(plan, api_results, user_message, locale, history, history_for_prompt)
        else:
            endpoints = plan.get("endpoints", [])
            logger.info(f"LLM selected endpoints: {endpoints}")
            api_results = await self.api_executor.execute(endpoints)
            response = await self.response_formatter.format(plan, api_results, user_message, locale, history, history_for_prompt)
        
        # Update chat history
        history.append({"user": user_message, "assistant": response})
        self.chat_context[user_id] = history[-10:]
        logger.info(f"Updated chat history for user {user_id}: {len(self.chat_context[user_id])} messages")
        return response

    async def call_tool(self, name, *args, **kwargs):
        tool = self.tools.get(name)
        if tool is None:
            raise AttributeError(f"Tool {name} not found")
        return await tool(*args, **kwargs)
