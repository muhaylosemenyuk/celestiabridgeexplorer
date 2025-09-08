import importlib.util
import inspect
from typing import Dict, Any

class APIRegistry:
    def __init__(self):
        self.endpoints = {}
        self._load_fastapi_endpoints("api_main.py")
        self._load_cosmos_endpoints("services/cosmos_api.py")

    def _load_fastapi_endpoints(self, path: str):
        spec = importlib.util.spec_from_file_location("api_main", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        app = getattr(module, "app", None)
        if not app:
            return
        for route in getattr(app, "routes", []):
            if hasattr(route, "endpoint"):
                endpoint_name = route.path.replace("/", "_").strip("_")
                self.endpoints[endpoint_name] = {
                    "api_type": "local",
                    "method": list(route.methods)[0] if route.methods else "GET",
                    "description": route.endpoint.__doc__ or "Local API endpoint",
                    "parameters": list(inspect.signature(route.endpoint).parameters.keys()),
                    "url": route.path
                }

    def _load_cosmos_endpoints(self, path: str):
        spec = importlib.util.spec_from_file_location("cosmos_api", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for name, func in inspect.getmembers(module, inspect.isfunction):
            if name.startswith("get_"):
                self.endpoints[name] = {
                    "api_type": "cosmos",
                    "method": "GET",
                    "description": func.__doc__ or f"Cosmos API: {name}",
                    "parameters": list(inspect.signature(func).parameters.keys()),
                    "function": name
                }

    def get_llm_docs(self) -> str:
        docs = []
        for name, info in self.endpoints.items():
            params_str = ", ".join(info["parameters"])
            docs.append(f"- {name} ({info['api_type']}): {info['description']}\n  Parameters: {params_str}")
        return "\n".join(docs)

    def get_endpoint(self, name: str) -> Dict[str, Any]:
        return self.endpoints.get(name, {})
