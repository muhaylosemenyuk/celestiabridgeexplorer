from data_sources.otel import fetch_otel_metrics, parse_otel_metrics
from models.metric import Metric
from services.db import SessionLocal
from datetime import datetime

def import_metrics_to_db(metric_names=None):
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
    finally:
        session.close() 