from services.db import engine
from sqlalchemy import text

table_names = ['nodes', 'metrics', 'chain', 'releases', 'balance_history']

with engine.connect() as conn:
    result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
    tables = [row[0] for row in result]
    print('Tables in DB:', tables)
    for table in table_names:
        if table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"{table}: {count} records") 