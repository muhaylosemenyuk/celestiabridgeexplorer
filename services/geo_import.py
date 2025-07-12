from data_sources.geo_csv import read_geo_csv
from models.node import Node
from services.db import SessionLocal

def import_geo_to_db():
    nodes = read_geo_csv()
    session = SessionLocal()
    try:
        # Clear the table before import (updatable data)
        session.query(Node).delete()
        for n in nodes:
            if not n['peer_id']:
                continue
            node = Node(**n)
            session.add(node)
        session.commit()
    finally:
        session.close() 