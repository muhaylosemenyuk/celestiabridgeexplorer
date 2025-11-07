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
from services.balance_import import import_balances_to_db
from services.balance_export import export_balance_summary_json
from services.validator_import import ValidatorImporter
from services.validator_export import ValidatorExporter
from services.delegation_import import import_delegations_to_db
from services.delegation_export import DelegationExporter

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

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
    """Import bridge nodes from location.json into the database (nodes table)."""
    import_geo_to_db()
    click.echo("Bridge nodes data imported to DB from location.json.")

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
    """
    Show first 10 records from a table.
    
    Available tables: nodes, metrics, chain, releases, balance_history, validators, delegations, delegator_stats
    
    Examples:
      python main.py show_table nodes
      python main.py show_table metrics
      python main.py show_table validators
    """
    # List of valid user tables (excluding system tables)
    valid_tables = [
        'nodes', 'metrics', 'chain', 'releases', 
        'balance_history', 'validators', 'delegations', 'delegator_stats'
    ]
    
    with engine.connect() as conn:
        # Check if table exists
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table"), {"table": table})
        if not result.fetchone():
            click.echo(f"Error: Table '{table}' does not exist.\n", err=True)
            click.echo("Available tables:")
            table_descriptions = {
                'nodes': 'Bridge node information with geographic and provider data',
                'metrics': 'OpenTelemetry metrics from bridge nodes',
                'chain': 'Chain metrics (stake, delegators, inflation, etc.)',
                'releases': 'Celestia software releases',
                'balance_history': 'Wallet balance history',
                'validators': 'Validator data from Cosmos API',
                'delegations': 'Delegation records',
                'delegator_stats': 'Delegator statistics (if available)'
            }
            for tbl in valid_tables:
                desc = table_descriptions.get(tbl, '')
                click.echo(f"  {tbl:20} - {desc}")
            click.echo("\nExample: python main.py show_table nodes")
            return
        
        # Get table data
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

@cli.command(name="import_balances")
@click.option('--limit', default=None, type=int, help='Limit number of addresses to process (for testing)')
def import_balances(limit):
    """Import wallet balances from Cosmos API to database."""
    
    result = import_balances_to_db(limit=limit)
    click.echo(f"Balances imported: {result}")

@cli.command(name="export_balances")
@click.option('--out', default=None, help='File to save JSON (optional)')
def export_balances(out):
    """Export balance data as JSON."""
    js = export_balance_summary_json(out_path=out)
    if out:
        click.echo(f"Balances exported to {out}")
    else:
        click.echo(js)


# ===== VALIDATOR COMMANDS =====

@cli.command(name="import_validators")
def import_validators():
    """Import all validators from Celestia API into the database using parallel processing."""
    click.echo("Starting validator import with parallel processing...")
    
    importer = ValidatorImporter()
    stats = importer.import_all_validators()
    
    click.echo(f"Import completed:")
    click.echo(f"  Total processed: {stats['total_processed']}")
    click.echo(f"  Successful: {stats['successful']}")
    click.echo(f"  Failed: {stats['failed']}")
    
    if stats['errors']:
        click.echo("Errors:")
        for error in stats['errors'][:5]:  # Show first 5 errors
            click.echo(f"  - {error}")
        if len(stats['errors']) > 5:
            click.echo(f"  ... and {len(stats['errors']) - 5} more errors")


@cli.command(name="export_validators")
@click.option('--format', 'export_format', type=click.Choice(['json', 'csv']), default='json', help='Export format')
@click.option('--out', default=None, help='Output file (optional)')
def export_validators(export_format, out):
    """Export validators data."""
    click.echo(f"Exporting validators in {export_format.upper()} format...")
    
    exporter = ValidatorExporter()
    
    if export_format == 'json':
        output_file = exporter.export_to_json(output_file=out)
    else:  # csv
        output_file = exporter.export_to_csv(output_file=out)
    
    click.echo(f"Validators exported to: {output_file}")


# ===== DELEGATION COMMANDS =====

@cli.command(name="import_delegations")
@click.option('--limit', default=None, type=int, help='Limit number of validators to process (for testing)')
def import_delegations(limit):
    """Import delegation data from Cosmos API to database."""
    
    result = import_delegations_to_db(limit=limit)
    click.echo(f"Delegations imported: {result}")


@cli.command(name="export_delegations")
@click.option('--format', 'export_format', type=click.Choice(['json', 'csv']), default='json', help='Export format')
@click.option('--out', default=None, help='Output file (optional)')
def export_delegations(export_format, out):
    """Export delegation data."""
    click.echo(f"Exporting delegations in {export_format.upper()} format...")
    
    exporter = DelegationExporter()
    
    if export_format == 'json':
        output_file = exporter.export_to_json(output_file=out)
    else:  # csv
        output_file = exporter.export_to_csv(output_file=out)
    
    click.echo(f"Delegations exported to: {output_file}")


@cli.command(name="delegation_stats")
def delegation_stats():
    """Show delegation statistics."""
    import logging
    logging.getLogger().setLevel(logging.ERROR)
    _show_delegation_stats()


@cli.command(name="validator_stats")
def validator_stats():
    """Show validator statistics."""
    import logging
    logging.getLogger().setLevel(logging.ERROR)
    _show_validator_stats()


@cli.command(name="balance_stats")
def balance_stats():
    """Show balance statistics."""
    import logging
    logging.getLogger().setLevel(logging.ERROR)
    _show_balance_stats()


@cli.command(name="all_stats")
def all_stats():
    """Show statistics for all tables."""
    import logging
    logging.getLogger().setLevel(logging.ERROR)
    
    click.echo("=" * 60)
    click.echo("📊 CELESTIA BRIDGE EXPLORER - COMPREHENSIVE STATISTICS")
    click.echo("=" * 60)
    
    # Delegation statistics
    click.echo("\n🔗 DELEGATION STATISTICS")
    click.echo("-" * 30)
    _show_delegation_stats()
    
    # Validator statistics
    click.echo("\n🏛️ VALIDATOR STATISTICS")
    click.echo("-" * 30)
    _show_validator_stats()
    
    # Balance statistics
    click.echo("\n💰 BALANCE STATISTICS")
    click.echo("-" * 30)
    _show_balance_stats()
    
    click.echo("\n" + "=" * 60)
    click.echo("✅ Statistics generation completed!")
    click.echo("=" * 60)


def _show_delegation_stats():
    """Internal function to show delegation statistics."""
    exporter = DelegationExporter()
    stats = exporter.get_delegation_statistics()
    
    # Extract total statistics
    total_stats = stats.get('total_statistics', {})
    total_results = total_stats.get('results', [])
    
    if total_results:
        total_count = total_results[0].get('count', 0)
        total_amount = total_results[0].get('sum_amount_tia', 0) or 0
        avg_amount = total_results[0].get('avg_amount_tia', 0) or 0
        max_amount = total_results[0].get('max_amount_tia', 0) or 0
        min_amount = total_results[0].get('min_amount_tia', 0) or 0
        
        click.echo("Delegation Statistics:")
        click.echo(f"  📊 Total delegations: {total_count:,}")
        click.echo(f"  💰 Total amount: {float(total_amount):,.2f} TIA")
        click.echo(f"  📈 Average delegation: {float(avg_amount):,.2f} TIA")
        click.echo(f"  🔝 Largest delegation: {float(max_amount):,.2f} TIA")
        click.echo(f"  🔻 Smallest delegation: {float(min_amount):,.6f} TIA")
        click.echo(f"  👥 Top delegators: {len(stats.get('top_delegators', []))} records")
        click.echo(f"  🏛️ Top validators: {len(stats.get('top_validators', []))} records")
        click.echo(f"  📅 Daily statistics: {len(stats.get('date_statistics', []))} records")
    else:
        click.echo("No delegation statistics available.")


def _show_validator_stats():
    """Internal function to show validator statistics."""
    from services.universal_db_aggregator import aggregate_db_data
    from models.validator import Validator
    
    # Get validator statistics
    stats = aggregate_db_data(
        model_class=Validator,
        aggregations=[
            {"type": "count"},
            {"type": "sum", "field": "tokens"},
            {"type": "avg", "field": "tokens"},
            {"type": "max", "field": "tokens"},
            {"type": "min", "field": "tokens"},
            {"type": "sum", "field": "voting_power"},
            {"type": "avg", "field": "uptime_percent"}
        ],
        return_format="aggregated"
    )
    
    # Get status distribution
    status_stats = aggregate_db_data(
        model_class=Validator,
        group_by=["status"],
        aggregations=[{"type": "count"}],
        return_format="list"
    )
    
    # Get jailed vs active
    jailed_stats = aggregate_db_data(
        model_class=Validator,
        group_by=["jailed"],
        aggregations=[{"type": "count"}],
        return_format="list"
    )
    
    total_results = stats.get('results', [])
    if total_results:
        result = total_results[0]
        total_count = result.get('count', 0)
        total_tokens = result.get('sum_tokens', 0)
        avg_tokens = result.get('avg_tokens', 0)
        max_tokens = result.get('max_tokens', 0)
        min_tokens = result.get('min_tokens', 0)
        total_voting_power = result.get('sum_voting_power', 0)
        avg_uptime = result.get('avg_uptime_percent', 0)
        
        click.echo("Validator Statistics:")
        click.echo(f"  🏛️ Total validators: {total_count:,}")
        click.echo(f"  💰 Total tokens: {float(total_tokens):,.2f} TIA")
        click.echo(f"  📈 Average tokens: {float(avg_tokens):,.2f} TIA")
        click.echo(f"  🔝 Largest stake: {float(max_tokens):,.2f} TIA")
        click.echo(f"  🔻 Smallest stake: {float(min_tokens):,.2f} TIA")
        click.echo(f"  ⚡ Total voting power: {float(total_voting_power):,.0f}")
        click.echo(f"  📊 Average uptime: {float(avg_uptime):.2f}%")
        
        # Status distribution
        status_results = status_stats.get('results', [])
        if status_results:
            click.echo("  📋 Status distribution:")
            for status in status_results:
                status_name = status.get('status', 'Unknown')
                count = status.get('count', 0)
                click.echo(f"    - {status_name}: {count}")
        
        # Jailed vs Active
        jailed_results = jailed_stats.get('results', [])
        if jailed_results:
            click.echo("  🔒 Jailed status:")
            for jailed in jailed_results:
                is_jailed = "Jailed" if jailed.get('jailed') else "Active"
                count = jailed.get('count', 0)
                click.echo(f"    - {is_jailed}: {count}")
    else:
        click.echo("No validator statistics available.")


def _show_balance_stats():
    """Internal function to show balance statistics."""
    from services.universal_db_aggregator import aggregate_db_data
    from models.balance import BalanceHistory
    
    # Get balance statistics
    stats = aggregate_db_data(
        model_class=BalanceHistory,
        aggregations=[
            {"type": "count"},
            {"type": "sum", "field": "balance_tia"},
            {"type": "avg", "field": "balance_tia"},
            {"type": "max", "field": "balance_tia"},
            {"type": "min", "field": "balance_tia"}
        ],
        return_format="aggregated"
    )
    
    # Get balance by date
    date_stats = aggregate_db_data(
        model_class=BalanceHistory,
        group_by=["date"],
        aggregations=[
            {"type": "count"},
            {"type": "sum", "field": "balance_tia"}
        ],
        order_by={"date": "desc"},
        limit=10,
        return_format="list"
    )
    
    # Get top addresses by balance
    top_addresses = aggregate_db_data(
        model_class=BalanceHistory,
        group_by=["address"],
        aggregations=[
            {"type": "count"},
            {"type": "sum", "field": "balance_tia"}
        ],
        order_by={"sum_balance_tia": "desc"},
        limit=10,
        return_format="list"
    )
    
    total_results = stats.get('results', [])
    if total_results:
        result = total_results[0]
        total_count = result.get('count', 0)
        total_balance = result.get('sum_balance_tia', 0)
        avg_balance = result.get('avg_balance_tia', 0)
        max_balance = result.get('max_balance_tia', 0)
        min_balance = result.get('min_balance_tia', 0)
        
        click.echo("Balance Statistics:")
        click.echo(f"  📊 Total records: {total_count:,}")
        click.echo(f"  💰 Total balance: {float(total_balance):,.2f} TIA")
        click.echo(f"  📈 Average balance: {float(avg_balance):,.2f} TIA")
        click.echo(f"  🔝 Largest balance: {float(max_balance):,.2f} TIA")
        click.echo(f"  🔻 Smallest balance: {float(min_balance):,.6f} TIA")
        
        # Recent dates
        date_results = date_stats.get('results', [])
        if date_results:
            click.echo("  📅 Recent dates:")
            for date_info in date_results[:5]:
                date = date_info.get('date', 'Unknown')
                count = date_info.get('count', 0)
                total = date_info.get('sum_balance_tia', 0)
                click.echo(f"    - {date}: {count} records, {float(total):,.2f} TIA")
        
        # Top addresses
        top_results = top_addresses.get('results', [])
        if top_results:
            click.echo("  🏆 Top addresses:")
            # Sort by balance descending and show top 5
            sorted_addresses = sorted(top_results, key=lambda x: float(x.get('sum_balance_tia', 0)), reverse=True)
            for addr in sorted_addresses[:5]:
                address = addr.get('address', 'Unknown')
                balance = addr.get('sum_balance_tia', 0)
                click.echo(f"    - {address[:20]}...: {float(balance):,.2f} TIA")
    else:
        click.echo("No balance statistics available.")


if __name__ == "__main__":
    cli()
