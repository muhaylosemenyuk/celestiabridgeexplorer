from services.db import SessionLocal
from models.release import Release
import json

def export_releases_json(out_path=None):
    session = SessionLocal()
    try:
        releases = session.query(Release).order_by(Release.published_at.desc()).all()
        data = [
            {
                'version': r.version,
                'published_at': r.published_at.isoformat() if r.published_at else None,
                'announce_str': r.announce_str,
                'deadline_str': r.deadline_str
            }
            for r in releases
        ]
        js = json.dumps(data, indent=2)
        if out_path:
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(js)
        return js
    finally:
        session.close() 