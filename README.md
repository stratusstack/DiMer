# DiMer — Data Diff CLI

DiMer is a universal data diff tool — think `git diff` for database tables. It connects to two data sources, compares their tables row by row, and reports what changed: rows added, deleted, or modified.

It ships as an interactive CLI (`dimer-diff`) and a Python library (`dimer`). It supports Snowflake, PostgreSQL, MySQL, BigQuery, Databricks, CSV, and Parquet out of the box, with automatic connection method fallback, diff history persistence, and three comparison algorithms for tables of any size.

---

## Installation

```bash
# Install with all connector extras (recommended)
pip install dimer[all]

# Or install only what you need
pip install dimer[snowflake]
pip install dimer[postgresql]
pip install dimer[mysql]

# Developers
pip install -e ".[dev]"
```

---

## Interactive CLI (quickest path)

```bash
dimer-diff
# or
python -m dimer

# Enable debug logging and full exception tracebacks
python -m dimer -dev
```

The CLI walks you through four steps:

1. **Select sources** — choose the data source type for each side
2. **Verify `.env`** — checks all required credentials, retries until complete
3. **Establish connections** — connects using the best available driver
4. **Compare tables** — enter table names, join keys, pick algorithm, run diff

After each diff you are prompted to save the results to the diff history database.

### Example session

```
── Step 1: Select data sources ────────────────────────────
  Target 1 source:
    1.  snowflake
    2.  postgresql
    ...

── Step 4: Asset comparison ────────────────────────────────
  Target 1 (postgresql) — table name
    > public.orders

  Detecting join keys for Target 1 (public.orders)...
    ✓  Primary keys detected: id
    Use these as join keys? [Y/n]:

  Algorithm selection
  Source row count : 2,450,000
  ⚠  Large table detected (2,450,000 rows). BISECTION algorithm recommended.
  Use BISECTION algorithm? [Y/n]:
  Bisection key column [id]:
  Threshold rows/segment [1000]:

  ──────────────────────────────────────────────────────
  Source  : postgresql           public.orders
  Target  : snowflake            PUBLIC.ORDERS
  Keys    : id  ←→  id
  Algorithm: BISECTION  (key=id, threshold=1000)
  ──────────────────────────────────────────────────────

  Run diff? [Y/n]:
  Running comparison...

  ──────────────────────────────────────────────────────
  ✗  MISMATCH  — tables differ
  Algorithm      : BISECTION
  Elapsed        : 3.41s
  Segments       : 16 initial, 2 differing
  Depth          : 1
  Source rows    : 2,450,000
  Target rows    : 2,450,001
  Added          : 1  (in target, not in source)
  Modified       : 3  (values differ)
  Matched        : 2,449,996
  ──────────────────────────────────────────────────────

  Save results? [Y/n]:
```

---

## Diff Algorithms

DiMer selects the algorithm automatically based on the data sources involved. One algorithm requires explicit opt-in.

| Algorithm | When used | How |
|---|---|---|
| `JOIN_DIFF` | Same database instance | SQL JOINs only — no data leaves the DB |
| `HASH_DIFF` | Different DB instances (default) | Narrow key+hash fetch, then targeted row fetch |
| `BISECTION` | Explicit opt-in (large tables) | NTILE segment hashing — fetches only differing buckets |
| `CROSS_DB_DIFF` | Legacy / direct call only | Full table fetch into Python |

**BISECTION** is auto-suggested by the CLI when the source table exceeds 1 million rows. To activate it in code, set `use_bisection=True` in the config:

```python
from dimer.core.models import BisectionConfig

db1: BisectionConfig = {
    "fq_table_name": "public.orders",
    "keys": ["id"],
    "use_bisection": True,
    "bisection_key": "id",       # sortable key for NTILE (defaults to keys[0])
    "bisection_threshold": 1000, # rows/segment before a warning is issued (default: 1000)
}
```

For a full explanation of each algorithm — including step-by-step SQL, data transfer analysis, and when to use each one — see **[ALGO.md](ALGO.md)**.

---

## Python API

### Connecting to a data source

```python
from dimer.core.factory import ConnectorFactory
from dimer.core.models import ConnectionConfig

config = ConnectionConfig(
    host="localhost",
    port=5432,
    username="user",
    password="password",
    database="mydb",
    schema_name="public",
)

connector = ConnectorFactory.create_connector("postgresql", config)
connector.connect()  # tries AsyncPG → psycopg2 → SQLAlchemy automatically

metadata = connector.get_table_metadata("orders")
print(f"{len(metadata.columns)} columns, {metadata.row_count} rows")

connector.close()
```

### Comparing two tables

```python
from dimer.core.compare import Diffcheck
from dimer.core.models import ComparisonConfig

db1: ComparisonConfig = {"fq_table_name": "public.orders", "keys": ["id"]}
db2: ComparisonConfig = {"fq_table_name": "PUBLIC.ORDERS", "keys": ["ID"]}

result = Diffcheck(connector1, connector2, db1, db2).compare()

print(f"Match: {result.match}")
print(f"Algorithm: {result.algorithm}")
print(f"Added: {result.summary.added_count}")
print(f"Deleted: {result.summary.deleted_count}")
print(f"Modified: {result.summary.modified_count}")

for row in result.modified_rows():
    print(row.key_values, row.mismatched_columns)
```

### Connection Manager

```python
from dimer.core.manager import ConnectionManager

manager = ConnectionManager()

connector = manager.create_connection(
    connection_id="prod-postgres",
    source_type="postgresql",
    connection_config=config,
)

if manager.test_connection("prod-postgres"):
    conn = manager.get_connection("prod-postgres")

manager.close_all()
```

---

## Supported Data Sources

| Source | Aliases | Connection methods (in preference order) |
|--------|---------|------------------------------------------|
| Snowflake | — | Arrow → Native → SQLAlchemy |
| PostgreSQL | `postgres` | AsyncPG → psycopg2 → SQLAlchemy |
| MySQL | — | mysql-connector → PyMySQL → SQLAlchemy |
| BigQuery | `bq` | BigQuery Storage API → Native → SQLAlchemy |
| Databricks | — | Databricks Connect → Native → SQLAlchemy |
| CSV | — | pandas |
| Parquet | — | PyArrow → pandas |

---

## Environment Variables

Create a `.env` file (see `.env.example`):

```bash
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secret
POSTGRES_DATABASE=mydb

# Snowflake
SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=myuser
SNOWFLAKE_PASSWORD=secret
SNOWFLAKE_DATABASE=MYDB
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN

# MySQL
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=secret
MYSQL_DATABASE=mydb

# BigQuery
BIGQUERY_PROJECT_ID=my-gcp-project
BIGQUERY_DATASET=my_dataset
BIGQUERY_CREDENTIALS_PATH=/path/to/key.json

# Databricks
DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxx

# Diff history persistence (optional — defaults to ~/.dimer/dimer.db)
DIMER_DB_URL=sqlite:///~/.dimer/dimer.db
# DIMER_DB_URL=postgresql://user:pass@host/dimer
```

---

## Diff History Persistence

DiMer automatically stores diff history for audit trails and trend analysis.

| Backend | URL format | When to use |
|---------|-----------|-------------|
| SQLite (default) | `sqlite:///~/.dimer/dimer.db` | Local / single-user |
| PostgreSQL | `postgresql://user:pass@host/dimer` | Team / production |

Set `DIMER_DB_URL` to switch. If unset, SQLite at `~/.dimer/dimer.db` is used automatically (the directory is created if it does not exist).

**What gets saved per run:**

| Table | Contents |
|-------|----------|
| `diff_run` | timestamp, algorithm, elapsed time, match result, algorithm metadata (e.g. bisection segment stats) |
| `diff_result` | aggregate counts: added, deleted, modified, matched |
| `diff_row` | up to 100 individual differing rows with key values and mismatched columns |
| `diff_job` | the table pair + key columns (reused across runs) |
| `project_source` | connection host/port/database — credentials are never stored |

**Retention:** after saving, DiMer optionally prunes old runs, keeping only the N most recent for each job.

---

## Testing

```bash
# All tests
pytest

# By category
pytest -m unit
pytest -m integration

# With coverage
pytest --cov=dimer --cov-report=term-missing

# Single connector integration tests (requires real credentials in .env)
pytest tests/test_postgres_integration.py -v -s
```

---

## Contributing

### Adding a new connector

1. Create a directory under `dimer/connectors/<source>/`
2. Subclass `DataSourceConnector` from `dimer.core.base`
3. Implement `get_required_params()`, `get_connection_methods()`, and a `_connect_<method>()` for each
4. Define `DIALECTS` with five keys: `hash`, `concatenation`, `cast_to_text`, `aggregate_hash` (required for bisection), and optionally `IDENTIFIER_CASE`
5. Register in `dimer/connectors/<source>/__init__.py` and in `_auto_register_connectors()` in `factory.py`
6. Add unit and integration tests

### Running linters

```bash
black .
isort .
flake8
mypy dimer/
```

---

## License

MIT License — see `LICENSE` for details.
