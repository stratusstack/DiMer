# DiMer: Difference Modeler for Data Diffs

DiMer - Difference Modeler is a toolkit for  performing value-level comparisons between two datasets, such as database tables, to identify differences inRows added, removed, or modified. It acts like git diff for data, helping engineers validate data quality during migrations, testing, and monitoring. It allows for comparing data across different environments or databases. 

It provides comprehensive integration and connectivity module for data diffing applications that supports multiple data sources with intelligent fallback mechanisms.

## Features

- **Multiple Connection Strategies**: Each connector supports multiple connection methods with automatic fallback
- **Unified Type System**: Consistent data type mapping across different data sources
- **Performance Monitoring**: Built-in metrics collection and performance analysis
- **Connection Management**: Intelligent connection pooling and lifecycle management
- **Extensible Architecture**: Easy to add new data source connectors
- **Production Ready**: Comprehensive error handling, logging, and retry mechanisms

## Supported Data Sources

- **Snowflake**: Arrow, Snowpark, Native, SQLAlchemy, JDBC, ODBC
- **PostgreSQL**: AsyncPG, Psycopg2, SQLAlchemy
- **MySQL**: PyMySQL, MySQL Connector, SQLAlchemy
- **BigQuery**: Storage API, Standard Client
- **Databricks**: SQL Connector, Databricks Connect
- **File Sources**: Parquet, CSV

## Installation

### Basic Installation

```bash
pip install dimer
```

### With Specific Data Source Support

```bash
# Snowflake support
pip install dimer[snowflake]

# PostgreSQL support
pip install dimer[postgresql]

# All data sources
pip install dimer[all]

# Development dependencies
pip install dimer[dev]
```

## Quick Start

### Basic Usage

```python
from dimer.core.models import ConnectionConfig
from dimer.core.factory import ConnectorFactory

# Create connection configuration
config = ConnectionConfig(
    # different configuration parameters go here like host
)

# Create connector
connector = ConnectorFactory.create_connector('<database name>', config)

# Connect with automatic fallback
connector.connect()

# Get table metadata
metadata = connector.get_table_metadata('my_table')
print(f"Table has {len(metadata.columns)} columns")

# Sample data
sample_data = connector.get_sample_data('my_table', limit=10)
print(sample_data.head())

# Clean up
connector.close()
```

### Connection Manager

```python
from dimer.core.manager import ConnectionManager

manager = ConnectionManager()

# Create managed connection
connector = manager.create_connection(
    connection_id='<connection name>',
    source_type='<database name>',
    connection_config=config
)

# List all connections
connections = manager.list_connections()

# Test connection
if manager.test_connection('<connection name>'):
    print("Connection is healthy")

# Get connection for use
conn = manager.get_connection('<connection name>')

# Clean up all connections
manager.close_all()
```

### Performance Monitoring

```python
from dimer.metrics.collector import get_metrics_collector

# Get global metrics collector
collector = get_metrics_collector()

# Connection statistics
stats = collector.get_connection_statistics()
for source_method, performance in stats.items():
    print(f"{source_method}: {performance.success_rate:.1%} success rate")

# Method comparison for a specific source
comparison = collector.get_method_comparison('<database name>')
for method, data in comparison.items():
    print(f"{method}: avg {data['performance_stats'].avg_duration:.2f}s")
```

## Architecture

### Core Components

- **DataSourceConnector**: Abstract base class for all connectors
- **ConnectorFactory**: Factory pattern for creating connectors
- **ConnectionManager**: Connection lifecycle and pooling management
- **DataTypeMapper**: Unified type system across data sources
- **MetricsCollector**: Performance monitoring and analytics

### Connection Strategies

Each connector implements multiple connection methods in order of preference:

1. **Optimized Methods**: Native high-performance drivers (Arrow, AsyncPG)
2. **Standard Methods**: Common database adapters (psycopg2, native connectors)
3. **Fallback Methods**: Universal adapters (JDBC, ODBC)

### Type System

All data types are mapped to a common type system:

- `string`, `text` - Text data
- `int8`, `int16`, `int32`, `int64` - Integer types
- `float32`, `float64` - Floating point
- `decimal` - High precision numbers
- `boolean` - Boolean values
- `date`, `datetime`, `timestamp` - Date/time types
- `json`, `array`, `object` - Complex types

## Configuration

### Connection Configuration

```python
from dimer.core.models import ConnectionConfig

config = ConnectionConfig(
    host='localhost',
    port=5432,
    username='user',
    password='password',
    database='mydb',
    schema_name='public',

    # Connection pool settings
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,

    # Retry settings
    max_retries=3,
    retry_delay=1.0,
    backoff_factor=2.0,

    # Timeout settings
    connect_timeout=30,
    query_timeout=300,

    # Source-specific parameters
    extra_params={
        'ssl_mode': 'require',
        'application_name': 'dimer'
    }
)
```

### Environment Variables

```bash
# Snowflake
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=username
export SNOWFLAKE_PASSWORD=password
export SNOWFLAKE_DATABASE=DATABASE
export SNOWFLAKE_WAREHOUSE=WAREHOUSE
export SNOWFLAKE_ROLE=ROLE

# PostgreSQL
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=password
export POSTGRES_DATABASE=postgres
```

## Error Handling

DiMer provides comprehensive error handling with automatic fallback:

```python
try:
    connector = ConnectorFactory.create_connector('snowflake', config)
    connector.connect()  # Tries multiple methods automatically
except ConnectionError as e:
    print(f"All connection methods failed: {e}")

# Check which methods were attempted
metrics = connector.get_connection_metrics()
for attempt in metrics:
    print(f"{attempt['method']}: {'✓' if attempt['success'] else '✗'}")
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=dimer

# Run specific test categories
pytest -m unit
pytest -m integration
pytest -m slow
```

## Performance

### Connection Method Performance (typical)

| Data Source | Method | Avg Connect Time | Query Performance | Use Case |
|-------------|--------|------------------|-------------------|----------|
| Snowflake | Arrow | 1.2s | Excellent | Large datasets |
| Snowflake | Snowpark | 1.5s | Very Good | DataFrame ops |
| Snowflake | Native | 2.0s | Good | General use |
| PostgreSQL | AsyncPG | 0.1s | Excellent | High concurrency |
| PostgreSQL | Psycopg2 | 0.2s | Very Good | General use |

### Best Practices

- Use connection pooling for multiple queries
- Enable Arrow format for large Snowflake queries
- Use AsyncPG for high-concurrency PostgreSQL workloads
- Monitor connection metrics to optimize performance
- Set appropriate timeouts based on your use case

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-connector`)
3. Make changes and add tests
4. Run tests (`pytest`)
5. Submit a pull request

### Adding New Connectors

1. Create connector class inheriting from `DataSourceConnector`
2. Implement required abstract methods
3. Add connection methods in order of preference
4. Register with factory in `__init__.py`
5. Add comprehensive tests

## License

MIT License - see LICENSE file for details.

## Support

- **Documentation**: [docs.dimer.io](https://docs.dimer.io)
- **Issues**: [GitHub Issues](https://github.com/dimer/dimer/issues)
- **Discussions**: [GitHub Discussions](https://github.com/dimer/dimer/discussions)
