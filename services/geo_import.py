from data_sources.location_json import read_location_json
from models.node import Node
from services.db import SessionLocal
import logging

def import_geo_to_db():
    """
    Import bridge nodes from location.json into database.
    Enhanced JSON data including decentralization metrics and geographic distribution.
    """
    logging.info("Starting bridge nodes import from location.json...")
    
    nodes = read_location_json()
    if not nodes:
        logging.error("No nodes found in location.json")
        return
    
    session = SessionLocal()
    try:
        # Clear the table before import (updatable data)
        logging.info("Clearing existing nodes from database...")
        session.query(Node).delete()
        
        # Import new nodes
        imported_count = 0
        for n in nodes:
            if not n['peer_id']:
                logging.warning("Skipping node without peer_id")
                continue
            
            try:
                node = Node(**n)
                session.add(node)
                imported_count += 1
            except Exception as e:
                logging.error(f"Error creating node {n.get('peer_id', 'unknown')}: {e}")
                continue
        
        session.commit()
        logging.info(f"Successfully imported {imported_count} bridge nodes from location.json")
        
        # Log some statistics
        if imported_count > 0:
            _log_import_statistics(session)
        
    except Exception as e:
        session.rollback()
        logging.error(f"Error during import: {e}")
        raise
    finally:
        session.close()

def _log_import_statistics(session):
    """Log import statistics for monitoring"""
    try:
        # Get basic statistics
        total_nodes = session.query(Node).count()
        
        # Geographic distribution
        countries = session.query(Node.country).distinct().count()
        continents = session.query(Node.continent).distinct().count()
        cities = session.query(Node.city).distinct().count()
        
        # Provider distribution
        providers = session.query(Node.provider).distinct().count()
        
        
        # Rules statistics
        hetzner_count = session.query(Node).filter(Node.provider_hetzner == True).count()
        over_limit_countries = session.query(Node).filter(Node.country_over_limit == True).count()
        over_limit_continents = session.query(Node).filter(Node.continent_over_limit == True).count()
        over_limit_cities = session.query(Node).filter(Node.city_over_limit == True).count()
        over_limit_providers = session.query(Node).filter(Node.provider_over_limit == True).count()
        
        logging.info("=== IMPORT STATISTICS ===")
        logging.info(f"Total nodes: {total_nodes}")
        logging.info(f"Countries: {countries}")
        logging.info(f"Continents: {continents}")
        logging.info(f"Cities: {cities}")
        logging.info(f"Providers: {providers}")
        logging.info(f"Hetzner nodes: {hetzner_count}")
        logging.info(f"Countries over limit: {over_limit_countries}")
        logging.info(f"Continents over limit: {over_limit_continents}")
        logging.info(f"Cities over limit: {over_limit_cities}")
        logging.info(f"Providers over limit: {over_limit_providers}")
        logging.info("=========================")
        
    except Exception as e:
        logging.warning(f"Could not generate import statistics: {e}") 