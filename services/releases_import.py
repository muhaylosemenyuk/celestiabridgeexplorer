from data_sources.api import get_github_releases
from models.release import Release
from services.db import SessionLocal
from datetime import datetime

def import_releases_to_db():
    releases = get_github_releases()
    session = SessionLocal()
    try:
        # Clear the table before import (updatable data)
        session.query(Release).delete()
        for r in releases:
            version = r.get('tag_name')
            published_at = r.get('published_at')
            if not version or not published_at:
                continue
            release = Release(
                version=version,
                published_at=datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ"),
                announce_str=published_at,
                deadline_str=None
            )
            session.add(release)
        session.commit()
    finally:
        session.close() 