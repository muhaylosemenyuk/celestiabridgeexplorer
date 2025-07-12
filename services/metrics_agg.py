from services.db import SessionLocal
from models.metric import Metric
from sqlalchemy import func
from datetime import datetime, timedelta
import json

def aggregate_metrics(metric_name, period_hours=24):
    """Aggregate average metric value per instance for the last period (hours)."""
    session = SessionLocal()
    since = datetime.utcnow() - timedelta(hours=period_hours)
    try:
        q = (
            session.query(
                Metric.instance,
                func.avg(Metric.value).label('avg_value'),
                func.min(Metric.value).label('min_value'),
                func.max(Metric.value).label('max_value'),
                func.count(Metric.value).label('count')
            )
            .filter(Metric.metric_name == metric_name)
            .filter(Metric.timestamp >= since)
            .group_by(Metric.instance)
        )
        result = [
            {
                'instance': row.instance,
                'avg': row.avg_value,
                'min': row.min_value,
                'max': row.max_value,
                'count': row.count
            }
            for row in q
        ]
        return result
    finally:
        session.close()

def export_agg_json(metric_name, period_hours=24, out_path=None):
    data = aggregate_metrics(metric_name, period_hours)
    js = json.dumps(data, indent=2)
    if out_path:
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(js)
    return js 