import asyncio
import httpx
import importlib.util
from typing import List, Dict, Any
from services.paginated_aggregator import fetch_and_aggregate_paginated
import re

class APIExecutor:
    def __init__(self, api_registry, local_api_url="http://localhost:8000"):
        self.registry = api_registry
        self.local_api_url = local_api_url
        # Dynamically import cosmos_api.py
        spec = importlib.util.spec_from_file_location("cosmos_api", "services/cosmos_api.py")
        self.cosmos_api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.cosmos_api)

    def fill_endpoint_template(self, endpoint: str, params: dict) -> str:
        """
        Replace {param} in endpoint with values from params and remove them from params.
        """
        def repl(match):
            key = match.group(1)
            value = params.pop(key, None)
            return str(value) if value is not None else match.group(0)
        return re.sub(r"\{(\w+)\}", repl, endpoint)

    async def execute(self, endpoints: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = {}
        for ep in endpoints:
            params = ep.get("parameters", {}).copy()
            # Substitute values from previous results
            for k, v in params.items():
                if isinstance(v, str) and v.startswith("from_"):
                    ref_name = v.replace("from_", "")
                    ref_result = results.get(ref_name)
                    # If the result is a dict with key 'height', substitute it, otherwise use the result itself
                    if isinstance(ref_result, dict) and "height" in ref_result:
                        params[k] = ref_result["height"]
                    else:
                        params[k] = ref_result
            # Call endpoint with updated parameters
            result = await self._call_endpoint({**ep, "parameters": params})
            results[ep["name"]] = result
        return results

    async def _call_endpoint(self, endpoint: Dict[str, Any]) -> Any:
        info = self.registry.get_endpoint(endpoint["name"])
        params = endpoint.get("parameters", {}).copy()
        # --- Universal pagination and aggregation support ---
        pagination_agg = endpoint.get("pagination_aggregate")
        if pagination_agg:
            agg_params = pagination_agg.get("params", {}).copy()
            agg_endpoint = self.fill_endpoint_template(pagination_agg["endpoint"], agg_params)
            return fetch_and_aggregate_paginated(
                endpoint=agg_endpoint,
                params=agg_params,
                item_path=pagination_agg["item_path"],
                aggregate=pagination_agg.get("aggregate", "all"),
                aggregate_field=pagination_agg.get("aggregate_field"),
                top_n=pagination_agg.get("top_n"),
                sort_desc=pagination_agg.get("sort_desc", True),
                filter=pagination_agg.get("filter")
            )
        # --- Standard logic ---
        if info.get("api_type") == "local":
            url = f"{self.local_api_url}{info['url']}"
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, params=params)
                try:
                    return resp.json()
                except Exception:
                    return {"error": resp.text}
        elif info.get("api_type") == "cosmos":
            cosmos_endpoint = self.fill_endpoint_template(info.get("url", ""), params)
            func = getattr(self.cosmos_api, endpoint["name"], None)
            if func:
                return await asyncio.to_thread(func, **params)
            else:
                return {"error": f"Function {endpoint['name']} not found in cosmos_api.py"}
        else:
            return {"error": "Unknown API type"}
