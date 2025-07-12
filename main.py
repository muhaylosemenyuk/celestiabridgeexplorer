import click
from services.db import init_db as init_db_func
from services.geo_import import import_geo_to_db
from sqlalchemy import text
from services.db import engine
from services.metrics_import import import_metrics_to_db
from services.metrics_agg import export_agg_json
from services.chain_import import import_chain_to_db
from services.releases_import import import_releases_to_db
from services.releases_export import export_releases_json
from services.chain_export import export_chain_json
from services.node_export import export_nodes_json

@click.group()
def cli():
    """CelestiaBridge CLI entrypoint."""
    pass

@cli.command(name="init_db")
def init_db():
    """Initialize the database (create tables)."""
    init_db_func()
    click.echo("Database initialized.")

@cli.command(name="import_geo")
def import_geo():
    """Import geo-csv data into the database (nodes table)."""
    import_geo_to_db()
    click.echo("Geo data imported to DB.")

@cli.command(name="import_metrics")
def import_metrics():
    """Import Prometheus metrics into the database (metrics table)."""
    import_metrics_to_db()
    click.echo("Prometheus metrics imported to DB.")

@cli.command(name="import_chain")
def import_chain():
    """Import chain metrics (legacy) into the database."""
    import_chain_to_db()
    click.echo("Chain metrics imported to DB.")

@cli.command(name="import_releases")
def import_releases():
    """Import releases from GitHub into the database (releases table)."""
    import_releases_to_db()
    click.echo("Releases imported to DB.")

@cli.command(name="show_table")
@click.argument('table')
def show_table(table):
    """Show first 10 records from a table (e.g. nodes, metrics, chain, releases)."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table} LIMIT 10"))
        rows = result.fetchall()
        if not rows:
            click.echo(f"No records found in table '{table}'.")
            return
        # Print column names
        columns = result.keys()
        click.echo(f"Columns: {columns}")
        click.echo(f"Showing first 10 records from '{table}':")
        for row in rows:
            click.echo(str(dict(zip(columns, row))))

@cli.command(name="export_agg")
@click.argument('metric_name')
@click.option('--hours', default=24, help='Aggregation period in hours')
@click.option('--out', default=None, help='File to save JSON (optional)')
def export_agg(metric_name, hours, out):
    """Export aggregated metrics (avg/min/max/count per instance) as JSON."""
    js = export_agg_json(metric_name, period_hours=hours, out_path=out)
    click.echo(js)

@cli.command(name="export_releases")
@click.option('--out', default=None, help='File to save JSON (optional)')
def export_releases(out):
    """Export releases as JSON."""
    js = export_releases_json(out_path=out)
    click.echo(js)

@cli.command(name="export_chain")
@click.option('--out', default=None, help='File to save JSON (optional)')
@click.option('--limit', default=100, help='Number of latest records (default 100)')
def export_chain(out, limit):
    """Export chain metrics as JSON (legacy format)."""
    js = export_chain_json(out_path=out, limit=limit)
    if out:
        click.echo(f"Chain metrics exported to {out}")
    else:
        click.echo(js)

@cli.command(name="export_nodes")
@click.option('--out', default=None, help='File to save JSON (optional)')
def export_nodes(out):
    """Export nodes as JSON."""
    js = export_nodes_json(out_path=out)
    if out:
        click.echo(f"Nodes exported to {out}")
    else:
        click.echo(js)

if __name__ == "__main__":
    cli()
