from data_sources.otel import fetch_otel_metrics, parse_otel_metrics
from models.metric import Metric
from services.db import SessionLocal
from datetime import datetime

# List of core metrics to collect
TARGET_METRICS = [
    "process_runtime_go_mem_heap_alloc_bytes",
    "process_runtime_go_goroutines",
    "process_runtime_go_gc_pause_ns_sum",
    "eds_cache_0x40082a1b08_get_counter_total",
    "eds_store_put_time_histogram_sum",
    "eds_store_put_time_histogram_count",
    "eds_store_put_time_histogram_bucket",
    "shrex_eds_server_responses_total",
    "shrex_nd_server_responses_total",
    "hdr_sync_subjective_head_gauge",
    "hdr_store_head_height_gauge",
    "is_sync"  # Custom sync metric
]

def import_metrics_to_db(metric_names=None):
    """
    Import metrics to database. If metric_names is not specified, uses TARGET_METRICS.
    Calculates sync level for hdr_sync_subjective_head_gauge and hdr_store_head_height_gauge.
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
        
        # Group metrics by instance to calculate sync level
        metrics_by_instance = {}
        for m in metrics:
            instance = m['instance']
            if instance not in metrics_by_instance:
                metrics_by_instance[instance] = {}
            metrics_by_instance[instance][m['metric_name']] = m['value']
        
        # Calculate sync level for each instance
        sync_levels = {}
        for instance, instance_metrics in metrics_by_instance.items():
            sync_value = instance_metrics.get('hdr_sync_subjective_head_gauge')
            store_value = instance_metrics.get('hdr_store_head_height_gauge')
            
            if sync_value is not None and store_value is not None and store_value > 0:
                # Calculate sync percentage: (sync_value / store_value) * 100
                sync_percentage = (sync_value / store_value) * 100
                sync_levels[instance] = min(100.0, max(0.0, sync_percentage))  # Clamp between 0-100
            else:
                sync_levels[instance] = None
        
        # Import all metrics
        for m in metrics:
            metric = Metric(
                instance=m['instance'],
                metric_name=m['metric_name'],
                value=m['value'],
                timestamp=now
            )
            session.add(metric)
        
        # Add is_sync as separate metric for each instance
        for instance, sync_percentage in sync_levels.items():
            if sync_percentage is not None:
                sync_metric = Metric(
                    instance=instance,
                    metric_name='is_sync',
                    value=sync_percentage,
                    timestamp=now
                )
                session.add(sync_metric)
        
        session.commit()
        print(f"Imported {len(metrics)} metric records")
        
        # Print sync statistics
        sync_stats = [sync for sync in sync_levels.values() if sync is not None]
        if sync_stats:
            avg_sync = sum(sync_stats) / len(sync_stats)
            min_sync = min(sync_stats)
            max_sync = max(sync_stats)
            print(f"Sync statistics: avg={avg_sync:.2f}%, min={min_sync:.2f}%, max={max_sync:.2f}%")
        
    finally:
        session.close() 