# CelestiaBridge

## Project Description

CelestiaBridge is a modular backend platform for collecting, processing, aggregating, and exporting analytical data about the Celestia network. The system automatically gathers metrics from various sources (otel metrics, APIs, CSV, GitHub), normalizes and stores them in a structured database, and provides a unified CLI for import, export, and analytics. The main goal is to provide a single point of data collection and preparation for dashboards, monitoring, research, and analytics automation for Celestia.

**Key Features:**
- Collects metrics from otel metrics, APIs, CSV, GitHub Releases
- Unified pipeline for data processing and normalization
- Stores data in a structured database (PostgreSQL/SQLite)
- Aggregation and export to JSON for dashboards/BI/analytics
- Convenient CLI for import, export, and data viewing
- Flexible configuration via `.env` and `config/`
- Modular, clean, and testable codebase
- All docstrings and comments are in English
- All dependencies are managed via `requirements.txt`
- All error handling via try/except and logging
- All data models are in `models/`

**Intended Users:**
- Developers, analysts, DevOps, Celestia dashboard operators
- Anyone who needs to automatically collect, store, and analyze Celestia network data

---

## Quick Start

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or venv\Scripts\activate for Windows
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and edit as needed.
4. Run the CLI:
   ```bash
   python main.py --help
   ```

---

## CLI Usage

CelestiaBridge provides a unified CLI for importing, exporting, and viewing data. All commands are run as:

```bash
python main.py <command> [options]
```

### CLI Command Reference

- **init_db** — Initialize the database and create all required tables.
  - Example: `python main.py init_db`

- **import_geo** — Import geo-csv into the `nodes` table (updates node geoinfo).
  - Example: `python main.py import_geo`

- **import_metrics** — Import otel metrics into the `metrics` table.
  - Example: `python main.py import_metrics`

- **import_chain** — Import chain metrics (stake, delegators, inflation, etc.) into the `chain` table.
  - Example: `python main.py import_chain`

- **import_releases** — Import releases from GitHub into the `releases` table.
  - Example: `python main.py import_releases`

- **show_table <table>** — Show the first 10 records from the selected table (`nodes`, `metrics`, `chain`, `releases`, `delegator_stats`).
  - Example: `python main.py show_table nodes`

- **export_agg <metric_name> [--hours N] [--out FILE]** — Export an aggregated metric (e.g., latency) for the specified period to JSON.
  - Example: `python main.py export_agg latency --hours 48 --out latency_agg.json`

- **export_releases [--out FILE]** — Export all releases to JSON.
  - Example: `python main.py export_releases --out releases.json`

- **export_chain [--out FILE] [--limit N]** — Export chain metrics to JSON (legacy format).
  - Example: `python main.py export_chain --out chain.json --limit 50`

- **export_nodes [--out FILE]** — Export all nodes to JSON.
  - Example: `python main.py export_nodes --out nodes.json`

#### Help
To see all available commands and options:
```bash
python main.py --help
python main.py <command> --help
```

---

## Project Structure

- `data_sources/` — Modules for reading/parsing raw data (otel metrics, CSV, APIs)
- `models/` — Data models (`Node`, `Chain`, `Metric`, `Release`, etc.)
- `services/` — Business logic, integration, data merging, import/export to DB
- `config.py` — Main configuration file (project root)
- `.env` — Environment variables
- `main.py` — Entry point, CLI
- `context/` — Legacy folder, not used in new code

---

## Database Tables Structure

- **nodes**: Information about network nodes, including geo-location and metadata.
- **metrics**: Time-series metrics collected from otel metrics and other sources.
- **chain**: Chain-level metrics (stake, delegators, inflation, etc.).
- **releases**: GitHub release data for Celestia software.
- **delegator_stats**: Detailed statistics about delegators and their activity.

### Database Utility: check_db.py

The `check_db.py` utility prints the list of tables in the current database and shows the number of records in each main table (`nodes`, `metrics`, `chain`, `releases`).

**Usage:**
```bash
python check_db.py
```

---

## Data Flow Diagram

```mermaid
flowchart TD
    subgraph Sources
        A1["otel metrics API"]
        A2["Geo CSV"]
        A3["Chain/Validators API"]
        A4["GitHub Releases API"]
    end

    subgraph DataSources
        B1["parse_metrics"]
        B2["read_geo_csv"]
        B3["get_staked_tokens, get_validators_with_delegators, ..."]
        B4["get_github_releases"]
    end

    subgraph ImportServices["Import Services"]
        C1["import_metrics_to_db"]
        C2["import_geo_to_db"]
        C3["import_chain_to_db"]
        C4["import_releases_to_db"]
    end

    subgraph DB[Database]
        direction TB
        D1["metrics"]
        D2["nodes"]
        D3["chain"]
        D4["releases"]
        D5["delegator_stats"]
    end

    subgraph ExportServices["Export Services"]
        E1["export_agg_json"]
        E2["export_chain_json"]
        E3["export_nodes_json"]
        E4["export_releases_json"]
    end

    F1["Aggregated metrics JSON"]
    F2["Chain JSON"]
    F3["Nodes JSON"]
    F4["Releases JSON"]

    A1 --> B1 --> C1 --> D1
    A2 --> B2 --> C2 --> D2
    A3 --> B3 --> C3 --> D3
    A3 --> B3 --> C3 --> D5
    A4 --> B4 --> C4 --> D4

    D1 -.-> E1 --> F1
    D2 -.-> E3 --> F3
    D3 -.-> E2 --> F2
    D4 -.-> E4 --> F4
    D5 -.-> E1
```

---

## Architectural Principles

- All logic for reading, parsing, and normalizing raw data (otel metrics, CSV, APIs) is in `data_sources/`.
- All parsers have a unified interface: return `list[dict]` or `dict`, handle errors via try/except and logging.
- No hardcoded paths, keys, or constants — everything is managed via `config.py` and environment variables.
- All business logic, aggregation, and integration is in `services/`.
- All data models (`Node`, `Chain`, `Metric`, `Release`, etc.) are in `models/`.
- All dependencies are managed only via `requirements.txt`.
- All documentation and docstrings are in English.
- All error handling is performed via try/except and logging. Critical errors are logged but do not crash the pipeline (except for unrecoverable network errors).
- All configuration is handled via `.env` and `config.py`.
- The codebase is modular, clean, and testable.
- The legacy `context/` folder is not used in the new implementation.

---

## Error Handling & Logging

- All data reading/parsing functions in `data_sources/` use try/except and log errors, warnings, and info using the standard `logging` module.
- All critical errors are logged. The pipeline continues unless an unrecoverable error occurs (e.g., network fetch failure).
- No hardcoded variables; all configuration is via `.env` and `config.py`.

---

## Dependencies

All dependencies are managed via `requirements.txt`. Main dependencies include:
- SQLAlchemy
- requests
- python-dotenv
- pycountry
- pycountry-convert
- pandas
- pydantic
- click
- pytest
- logging (standard library)

---

## Usage Scenario

**Example:**
A DevOps engineer wants to automate the collection and aggregation of Celestia network metrics for a dashboard. They configure `.env`, run `python main.py import_metrics` and `python main.py export_agg latency --hours 24 --out latency.json`, and use the resulting JSON in their dashboard.

---

## API (FastAPI)

CelestiaBridge provides an open HTTP API to access all output data in JSON format.

### API Launch

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start the server:
   ```bash
   uvicorn api_main:app --reload
   ```
   The API will be available at http://127.0.0.1:8000

### Documentation
- Swagger UI: http://127.0.0.1:8000/docs
- OpenAPI JSON: http://127.0.0.1:8000/openapi.json

### Main Endpoints
- `GET /nodes` — list of nodes (with pagination)
- `GET /chain` — chain metrics (with pagination)
- `GET /metrics/aggregate?metric_name=...&hours=...` — aggregated metrics
- `GET /releases` — releases (with pagination)
- `GET /health` — API health check

#### Pagination Parameters
- `skip` — how many records to skip (default: 0)
- `limit` — how many records to return (default: 100, max: 1000)

#### Example Request
```bash
curl "http://127.0.0.1:8000/nodes?skip=0&limit=10"
```

---

## License

MIT License 
