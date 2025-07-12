import requests
import re
from config import OTEL_METRICS_URL
import logging

def fetch_otel_metrics(url=OTEL_METRICS_URL):
    """
    Download raw otel metrics as text. Handles network errors and logs them.
    """
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logging.error(f"Failed to fetch otel metrics from '{url}': {e}")
        return ""

def parse_otel_metrics(text, metric_names=None):
    """
    Parse required metrics from otel metrics text. Returns a list of dicts. Handles parsing errors and logs them.
    """
    if metric_names is None:
        metric_names = []
    pattern = re.compile(r'^(?P<name>\w+)(\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+eE0-9.]+)$', re.MULTILINE)
    result = []
    try:
        for m in pattern.finditer(text):
            name = m.group('name')
            if metric_names and name not in metric_names:
                continue
            labels = m.group('labels') or ''
            try:
                value = float(m.group('value'))
            except Exception as e:
                logging.warning(f"Invalid metric value: {m.group('value')}, error: {e}")
                continue
            peer_id = None
            for l in labels.split(','):
                if l.strip().startswith('instance='):
                    peer_id = l.split('=',1)[1].strip('" ')
            if not peer_id:
                continue
            result.append({'instance': peer_id, 'metric_name': name, 'value': value})
    except Exception as e:
        logging.error(f"Failed to parse otel metrics: {e}")
    return result 