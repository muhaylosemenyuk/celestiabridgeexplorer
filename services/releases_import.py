from data_sources.api import get_github_releases
from models.release import Release
from services.db import SessionLocal
from datetime import datetime

def determine_network_from_version(version):
    """
    Determine network type based on release version name.
    If version has a suffix (like -mocha, -arabica, -rc, -alpha, -beta), it's testnet.
    Only clean versions without any suffixes are considered mainnet.
    """
    if not version:
        return 'mainnet'
    
    # Check for testnet suffixes - any version with a dash is considered testnet
    # This includes: -mocha, -arabica, -testnet, -dev, -rc, -alpha, -beta, etc.
    if '-' in version:
        return 'testnet'
    
    return 'mainnet'

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
            
            network = determine_network_from_version(version)
            
            release = Release(
                version=version,
                published_at=datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ"),
                announce_str=published_at,
                deadline_str=None,
                network=network
            )
            session.add(release)
        session.commit()
    finally:
        session.close() 