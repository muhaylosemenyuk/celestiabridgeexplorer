from typing import Any, Dict, List, Optional
from services.cosmos_api import make_cosmos_request
import logging

def get_by_path(obj: dict, path: str) -> Any:
    """
    Traverse a nested dictionary using a dot-separated path.
    Example: get_by_path(item, 'balance.amount')
    """
    for part in path.split('.'):
        if isinstance(obj, dict):
            obj = obj.get(part, {})
        else:
            return None
    return obj if obj != {} else None

def apply_filter(items: list, filter_dict: Optional[dict]) -> list:
    logger = logging.getLogger("paginated_aggregator")
    if not filter_dict:
        logger.info("No filter applied.")
        return items
    field = filter_dict.get("field")
    op = filter_dict.get("operator")
    value = filter_dict.get("value")
    logger.info(f"Applying filter: field={field}, operator={op}, value={value}")
    before_count = len(items)
    def check(item):
        v = int(get_by_path(item, field) or 0)
        result = False
        if op == ">":
            result = v > value
        elif op == ">=":
            result = v >= value
        elif op == "<":
            result = v < value
        elif op == "<=":
            result = v <= value
        elif op == "==":
            result = v == value
        elif op == "!=":
            result = v != value
        return v, result
    filtered = []
    for item in items:
        v, passed = check(item)
        if passed:
            filtered.append(item)
    after_count = len(filtered)
    logger.info(f"Filter result: {after_count} of {before_count} items passed the filter.")
    if after_count > 0:
        logger.info(f"Example filtered value: {get_by_path(filtered[0], field)}")
    return filtered

def fetch_and_aggregate_paginated(
    endpoint: str,
    params: dict,
    item_path: str,
    aggregate: str = "all",
    aggregate_field: Optional[str] = None,
    top_n: Optional[int] = None,
    sort_desc: bool = True,
    filter: Optional[dict] = None
) -> Any:
    """
    Fetches all pages from a paginated Cosmos REST API endpoint and performs aggregation.
    Supports optional filtering before aggregation.
    """
    logger = logging.getLogger("paginated_aggregator")
    logger.info(f"[START] Pagination for endpoint: {endpoint}, params: {params}, item_path: {item_path}, aggregate: {aggregate}, aggregate_field: {aggregate_field}, top_n: {top_n}, sort_desc: {sort_desc}, filter: {filter}")
    results = []
    next_key = None
    page = 1
    while True:
        if next_key:
            params['pagination.key'] = next_key
        logger.info(f"Requesting page {page} from {endpoint} with params: {params}")
        resp = make_cosmos_request(endpoint, params)
        logger.info(f"Response from {endpoint} (page {page}): {str(resp)[:500]} ...")
        items = get_by_path(resp, item_path)
        if not isinstance(items, list):
            logger.warning(f"No items found at path '{item_path}' on page {page}.")
            break
        logger.info(f"Fetched {len(items)} items on page {page}.")
        results.extend(items)
        next_key = resp.get('pagination', {}).get('next_key')
        if not next_key:
            break
        page += 1
    logger.info(f"[END] Total items collected: {len(results)}")
    # Apply filter if present
    filtered = apply_filter(results, filter)
    logger.info(f"After filtering: {len(filtered)} items remain.")
    logger.info(f"Aggregation type: {aggregate}, field: {aggregate_field}")
    if aggregate == "all":
        logger.info(f"Returning all items (count: {len(filtered)})")
        return filtered
    elif aggregate == "sum":
        values = [int(get_by_path(item, aggregate_field) or 0) for item in filtered]
        logger.info(f"Sum values: {values[:10]}{'...' if len(values) > 10 else ''}")
        result = sum(values)
        logger.info(f"Sum result: {result}")
        return result
    elif aggregate == "count":
        logger.info(f"Count result: {len(filtered)}")
        return {"count": len(filtered), "items": filtered}
    elif aggregate == "unique":
        values = set(get_by_path(item, aggregate_field) for item in filtered)
        logger.info(f"Unique result count: {len(values)}; examples: {list(values)[:10]}")
        return list(values)
    elif aggregate == "top":
        values = [
            {"item": item, "value": int(get_by_path(item, aggregate_field) or 0)}
            for item in filtered
        ]
        logger.info(f"Top sort values: {[v['value'] for v in values[:10]]}{'...' if len(values) > 10 else ''}")
        values.sort(key=lambda x: x["value"], reverse=sort_desc)
        top_items = [v["item"] for v in values[:top_n]]
        logger.info(f"Top {top_n} result: {top_items}")
        return top_items
    elif aggregate == "max":
        values = [int(get_by_path(item, aggregate_field) or 0) for item in filtered]
        logger.info(f"Max values: {values[:10]}{'...' if len(values) > 10 else ''}")
        result = max(values) if values else None
        logger.info(f"Max result: {result}")
        return result
    elif aggregate == "min":
        values = [int(get_by_path(item, aggregate_field) or 0) for item in filtered]
        logger.info(f"Min values: {values[:10]}{'...' if len(values) > 10 else ''}")
        result = min(values) if values else None
        logger.info(f"Min result: {result}")
        return result
    else:
        logger.info(f"Returning all items (count: {len(filtered)})")
        return filtered
