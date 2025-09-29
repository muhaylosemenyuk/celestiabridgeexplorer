from data_sources.otel import fetch_otel_metrics, parse_otel_metrics
from models.metric import Metric
from services.db import SessionLocal
from datetime import datetime

# List of 9 core metrics to collect
TARGET_METRICS = [
    "process_runtime_go_mem_heap_alloc_bytes",
    "process_runtime_go_goroutines",
    "process_runtime_go_gc_pause_ns_sum",
    "eds_cache_0x40082a1b08_get_counter_total",
    "eds_store_put_time_histogram_sum",
    "eds_store_put_time_histogram_count",
    "eds_store_put_time_histogram_bucket",
    "shrex_eds_server_responses_total",
    "shrex_nd_server_responses_total"
]

def import_metrics_to_db(metric_names=None):
    """
    Import metrics to database. If metric_names is not specified, uses TARGET_METRICS.
    """
    if metric_names is None:
        metric_names = TARGET_METRICS
    
    text = fetch_otel_metrics()
    metrics = parse_otel_metrics(text, metric_names=metric_names)
    session = SessionLocal()
    now = datetime.utcnow()
    try:
        # Clear the table before import (updatable data)
        session.query(Metric).delete()
        for m in metrics:
            metric = Metric(
                instance=m['instance'],
                metric_name=m['metric_name'],
                value=m['value'],
                timestamp=now
            )
            session.add(metric)
        session.commit()
        print(f"Imported {len(metrics)} metric records")
    finally:
        session.close() 