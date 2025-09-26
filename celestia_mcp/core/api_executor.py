import asyncio
import httpx
import importlib.util
import inspect
from typing import List, Dict, Any
from services.paginated_aggregator import fetch_and_aggregate_paginated
import re

class APIExecutor:
    def __init__(self, api_registry, local_api_url="http://localhost:8002"):
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

    async def execute(self, endpoints_or_plan) -> Dict[str, Any]:
        # Check if this is a sequential request
        if isinstance(endpoints_or_plan, dict) and endpoints_or_plan.get("sequential"):
            return await self.execute_sequential(endpoints_or_plan)
        
        # Handle regular endpoints list
        endpoints = endpoints_or_plan if isinstance(endpoints_or_plan, list) else []
        
        # Handle regular single/multiple endpoints
        results = {}
        for i, ep in enumerate(endpoints):
            params = ep.get("parameters", {}).copy()
            
            # Handle multiple values in parameters
            params = self.handle_multiple_values(params)
            # Substitute values from previous results
            for k, v in params.items():
                if isinstance(v, str) and v.startswith("from_"):
                    # Parse reference like "from_validators[0].operator_address"
                    ref_parts = v.split("[")
                    if len(ref_parts) == 2:
                        ref_name = ref_parts[0].replace("from_", "")
                        index_part = ref_parts[1].split("]")[0]
                        field_path = ref_parts[1].split("]")[1].lstrip(".")
                        
                        try:
                            index = int(index_part)
                            ref_result = results.get(ref_name)
                            
                            if isinstance(ref_result, dict) and "results" in ref_result:
                                # Handle list results
                                if isinstance(ref_result["results"], list) and len(ref_result["results"]) > index:
                                    item = ref_result["results"][index]
                                    if field_path:
                                        # Navigate through field path like "operator_address"
                                        params[k] = item.get(field_path, "")
                                    else:
                                        params[k] = item
                                else:
                                    params[k] = ""
                            elif isinstance(ref_result, dict) and "height" in ref_result:
                                # Legacy height handling
                                params[k] = ref_result["height"]
                            else:
                                params[k] = ref_result
                        except (ValueError, IndexError, KeyError):
                            params[k] = ""
                    else:
                        # Simple reference without index
                        ref_name = v.replace("from_", "")
                        ref_result = results.get(ref_name)
                        if isinstance(ref_result, dict) and "height" in ref_result:
                            params[k] = ref_result["height"]
                        else:
                            params[k] = ref_result
            # Call endpoint with updated parameters
            result = await self._call_endpoint({**ep, "parameters": params})
            
            # Create unique key for each endpoint call to avoid overwriting
            endpoint_name = ep["name"]
            if endpoint_name in results:
                # If endpoint name already exists, create a list or use index
                if not isinstance(results[endpoint_name], list):
                    results[endpoint_name] = [results[endpoint_name]]
                results[endpoint_name].append(result)
            else:
                results[endpoint_name] = result
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
            timeout = httpx.Timeout(30.0)  # 30 seconds timeout
            async with httpx.AsyncClient(timeout=timeout) as client:
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
    
    async def execute_sequential(self, sequential_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute sequential requests with chaining support
        """
        steps = sequential_request.get("steps", [])
        results = {}
        
        for i, step in enumerate(steps):
            step_name = f"step{i+1}"
            step_params = step.get("parameters", {}).copy()
            
            # Substitute results from previous steps
            step_params = self.substitute_sequential_results(step_params, results)
            print(f"DEBUG: Step {i+1} original params: {step.get('parameters', {})}")
            print(f"DEBUG: Step {i+1} substituted params: {step_params}")
            
            # Handle multiple values in parameters
            step_params = self.handle_multiple_values(step_params)
            
            # For balances endpoint, include zero balances to get all addresses
            if step.get("name") == "balances" and "address" in step_params:
                step_params["include_zero_balances"] = True
            
            # Additional debugging for address parameter
            if 'address' in step_params:
                address_value = step_params['address']
                if isinstance(address_value, str) and ',' in address_value:
                    addresses = address_value.split(',')
                    print(f"DEBUG: Step {i+1} will query {len(addresses)} addresses: {addresses}")
                elif isinstance(address_value, str) and '{{' in address_value:
                    print(f"DEBUG: Step {i+1} has unresolved placeholder: {address_value}")
                else:
                    print(f"DEBUG: Step {i+1} will query single address: {address_value}")
            
            # Update step with substituted parameters
            step_copy = step.copy()
            step_copy["parameters"] = step_params
            
            # Execute the step
            step_result = await self._call_endpoint(step_copy)
            results[step_name] = step_result
            
            # Extract specific fields if requested
            extract_fields = step.get("extract_fields", [])
            if extract_fields and isinstance(step_result, dict) and "results" in step_result:
                extracted_data = self.extract_fields_from_results(step_result, extract_fields)
                results[f"{step_name}_extracted"] = extracted_data
        
        return results
    
    def substitute_sequential_results(self, params: Dict[str, Any], results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Substitute {{step1.field}} placeholders with actual results
        """
        substituted_params = {}
        
        for key, value in params.items():
            if isinstance(value, str) and "{{" in value:
                # Handle {{step1.field}} syntax
                print(f"DEBUG: Found placeholder {value} in parameter {key}")
                substituted_value = self.resolve_placeholder(value, results)
                print(f"DEBUG: Resolved to: {substituted_value}")
                substituted_params[key] = substituted_value
            else:
                substituted_params[key] = value
        
        return substituted_params
    
    def resolve_placeholder(self, placeholder: str, results: Dict[str, Any]) -> str:
        """
        Resolve {{step1.field}} placeholders to actual values
        """
        import re
        
        def replace_placeholder(match):
            full_match = match.group(0)
            content = match.group(1)
            
            # Parse step1.field format
            if "." in content:
                step_name, field_path = content.split(".", 1)
                
                # Get the step result
                step_result = results.get(step_name)
                if not step_result:
                    return placeholder  # Return original if step not found
                
                # Navigate through the field path
                if isinstance(step_result, dict) and "results" in step_result:
                    results_list = step_result["results"]
                    if isinstance(results_list, list) and len(results_list) > 0:
                        # Check if we need to handle multiple results
                        if field_path == "delegator_address" and len(results_list) > 1:
                            # For delegator_address, return comma-separated list
                            addresses = []
                            for result in results_list:
                                if isinstance(result, dict) and field_path in result:
                                    addresses.append(result[field_path])
                            return ",".join(addresses)
                        else:
                            # For single result or other fields, take the first
                            first_result = results_list[0]
                            if isinstance(first_result, dict):
                                return str(first_result.get(field_path, placeholder))
                
                return placeholder
            else:
                return placeholder
        
        # Replace all {{...}} patterns
        pattern = r'\{\{([^}]+)\}\}'
        return re.sub(pattern, replace_placeholder, placeholder)
    
    def extract_fields_from_results(self, result: Dict[str, Any], fields: List[str]) -> List[Dict[str, Any]]:
        """
        Extract specific fields from API results
        """
        if not isinstance(result, dict) or "results" not in result:
            return []
        
        results_list = result["results"]
        if not isinstance(results_list, list):
            return []
        
        extracted = []
        for item in results_list:
            if isinstance(item, dict):
                extracted_item = {}
                for field in fields:
                    if field in item:
                        extracted_item[field] = item[field]
                if extracted_item:
                    extracted.append(extracted_item)
        
        return extracted
    
    def handle_multiple_values(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle multiple values in parameters by splitting into groups of 10
        """
        MULTI_VALUE_LIMIT = 10
        processed_params = {}
        
        for key, value in params.items():
            if isinstance(value, str) and "," in value:
                # Split comma-separated values
                values = [v.strip() for v in value.split(",") if v.strip()]
                
                if len(values) <= MULTI_VALUE_LIMIT:
                    # For small lists, keep as comma-separated string
                    # The API endpoint will handle the 'in' filter conversion
                    processed_params[key] = ",".join(values)
                    print(f"DEBUG: Using comma-separated string for {key} with {len(values)} values")
                else:
                    # For large lists, use only the first group for now
                    first_group = values[:MULTI_VALUE_LIMIT]
                    processed_params[key] = ",".join(first_group)
                    print(f"DEBUG: Using first group of {len(first_group)} values for {key}")
            else:
                processed_params[key] = value
        
        return processed_params
