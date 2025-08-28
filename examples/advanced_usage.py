#!/usr/bin/env python3
"""
Advanced usage examples for DiffForge connectors.

This script demonstrates advanced features like using all connector types,
performance monitoring, connection management, and error handling.
"""

import os
import pandas as pd
import tempfile
from pathlib import Path
from typing import Dict, Any

from diffforge.core.models import ConnectionConfig
from diffforge.core.factory import ConnectorFactory
from diffforge.core.manager import ConnectionManager
from diffforge.metrics.collector import get_metrics_collector


def create_mysql_config() -> ConnectionConfig:
    """Create MySQL connection configuration."""
    return ConnectionConfig(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', '3306')),
        username=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', 'password'),
        database=os.getenv('MYSQL_DATABASE', 'test'),
        extra_params={
            'charset': 'utf8mb4',
            'ssl_disabled': True,
        }
    )


def create_bigquery_config() -> ConnectionConfig:
    """Create BigQuery connection configuration."""
    return ConnectionConfig(
        database=os.getenv('BIGQUERY_PROJECT_ID', 'your-gcp-project'),
        schema_name=os.getenv('BIGQUERY_DATASET', 'your_dataset'),
        extra_params={
            'location': os.getenv('BIGQUERY_LOCATION', 'US'),
            'credentials_path': os.getenv('BIGQUERY_CREDENTIALS_PATH'),
        }
    )


def create_databricks_config() -> ConnectionConfig:
    """Create Databricks connection configuration."""
    return ConnectionConfig(
        host=os.getenv('DATABRICKS_HOST', 'your-workspace.cloud.databricks.com'),
        password=os.getenv('DATABRICKS_TOKEN', 'your_access_token'),
        extra_params={
            'warehouse_id': os.getenv('DATABRICKS_WAREHOUSE_ID', 'your-warehouse-id'),
            'catalog': os.getenv('DATABRICKS_CATALOG', 'main'),
        }
    )


def create_sample_files():
    """Create sample Parquet and CSV files for testing."""
    temp_dir = tempfile.mkdtemp(prefix='diffforge_')
    
    # Sample data
    data = {
        'customer_id': range(1, 1001),
        'name': [f'Customer_{i}' for i in range(1, 1001)],
        'email': [f'customer{i}@example.com' for i in range(1, 1001)],
        'age': [20 + (i % 50) for i in range(1, 1001)],
        'city': ['New York', 'London', 'Tokyo', 'Paris', 'Berlin'][0:1000:200] * 200,
        'purchase_amount': [100 + (i * 1.5) % 1000 for i in range(1, 1001)],
    }
    df = pd.DataFrame(data)
    
    # Create Parquet files
    parquet_dir = Path(temp_dir) / 'parquet'
    parquet_dir.mkdir()
    
    # Split into multiple files by city
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        city_data.to_parquet(parquet_dir / f'customers_{city.lower().replace(" ", "_")}.parquet')
    
    # Create CSV files
    csv_dir = Path(temp_dir) / 'csv'
    csv_dir.mkdir()
    
    # Split by age groups
    young = df[df['age'] < 35]
    middle = df[(df['age'] >= 35) & (df['age'] < 50)]
    senior = df[df['age'] >= 50]
    
    young.to_csv(csv_dir / 'customers_young.csv', index=False)
    middle.to_csv(csv_dir / 'customers_middle.csv', index=False)  
    senior.to_csv(csv_dir / 'customers_senior.csv', index=False)
    
    return temp_dir, parquet_dir, csv_dir


def example_all_connector_types():
    """Example: Using all types of connectors."""
    print("=== Example: All Connector Types ===")
    
    # Create sample files
    temp_dir, parquet_dir, csv_dir = create_sample_files()
    
    try:
        # Test file-based connectors first (most likely to work)
        print("\n1. Parquet Connector")
        parquet_config = ConnectionConfig(host=str(parquet_dir))
        parquet_connector = ConnectorFactory.create_connector('parquet', parquet_config)
        
        with parquet_connector as conn:
            tables = conn.list_tables()
            print(f"   Found {len(tables)} Parquet files: {tables}")
            
            if tables:
                metadata = conn.get_table_metadata(tables[0])
                print(f"   Table {tables[0]}: {len(metadata.columns)} columns, {metadata.row_count} rows")
                
                sample = conn.get_sample_data(tables[0], limit=3)
                print(f"   Sample data shape: {sample.shape}")
        
        print("\n2. CSV Connector")
        csv_config = ConnectionConfig(
            host=str(csv_dir),
            extra_params={'encoding': 'utf-8', 'separator': ','}
        )
        csv_connector = ConnectorFactory.create_connector('csv', csv_config)
        
        with csv_connector as conn:
            tables = conn.list_tables()
            print(f"   Found {len(tables)} CSV files: {tables}")
            
            if tables:
                metadata = conn.get_table_metadata(tables[0])
                print(f"   Table {tables[0]}: {len(metadata.columns)} columns")
                
                sample = conn.get_sample_data(tables[0], limit=3)
                print(f"   Sample data shape: {sample.shape}")
        
        # Test database connectors (may fail without real connections)
        print("\n3. MySQL Connector (demo - may fail without real DB)")
        try:
            mysql_config = create_mysql_config()
            mysql_connector = ConnectorFactory.create_connector('mysql', mysql_config)
            print(f"   MySQL connector created: {mysql_connector}")
            print(f"   Connection methods: {[m.value for m in mysql_connector.connection_methods]}")
        except Exception as e:
            print(f"   MySQL connector demo failed: {e}")
        
        print("\n4. BigQuery Connector (demo - may fail without credentials)")
        try:
            bq_config = create_bigquery_config()
            bq_connector = ConnectorFactory.create_connector('bigquery', bq_config)
            print(f"   BigQuery connector created: {bq_connector}")
            print(f"   Connection methods: {[m.value for m in bq_connector.connection_methods]}")
        except Exception as e:
            print(f"   BigQuery connector demo failed: {e}")
        
        print("\n5. Databricks Connector (demo - may fail without credentials)")
        try:
            databricks_config = create_databricks_config()
            databricks_connector = ConnectorFactory.create_connector('databricks', databricks_config)
            print(f"   Databricks connector created: {databricks_connector}")
            print(f"   Connection methods: {[m.value for m in databricks_connector.connection_methods]}")
        except Exception as e:
            print(f"   Databricks connector demo failed: {e}")
    
    finally:
        # Clean up temporary files
        import shutil
        shutil.rmtree(temp_dir)


def example_advanced_connection_management():
    """Example: Advanced connection management with multiple sources."""
    print("\n=== Example: Advanced Connection Management ===")
    
    # Create sample files
    temp_dir, parquet_dir, csv_dir = create_sample_files()
    
    try:
        manager = ConnectionManager(default_pool_size=3, connection_timeout=600)
        
        # Create multiple connections of different types
        connections_config = [
            ('parquet_sales', 'parquet', ConnectionConfig(host=str(parquet_dir))),
            ('csv_demographics', 'csv', ConnectionConfig(host=str(csv_dir))),
        ]
        
        # Add real database connections if credentials are available
        if os.getenv('MYSQL_HOST'):
            connections_config.append(('mysql_prod', 'mysql', create_mysql_config()))
        
        if os.getenv('BIGQUERY_PROJECT_ID'):
            connections_config.append(('bq_analytics', 'bigquery', create_bigquery_config()))
        
        # Create all connections
        active_connections = []
        for conn_id, source_type, config in connections_config:
            try:
                connector = manager.create_connection(
                    connection_id=conn_id,
                    source_type=source_type,
                    connection_config=config,
                    connect_immediately=True
                )
                active_connections.append(conn_id)
                print(f"✓ Created connection: {conn_id} ({source_type})")
            except Exception as e:
                print(f"✗ Failed to create {conn_id}: {e}")
        
        # List all connections
        all_connections = manager.list_connections()
        print(f"\nActive connections: {len(all_connections)}")
        for conn in all_connections:
            print(f"  - {conn['connection_id']}: {conn['source_type']} "
                  f"({'connected' if conn['connected'] else 'disconnected'})")
        
        # Test each connection
        print(f"\nTesting connections:")
        for conn_id in active_connections:
            is_healthy = manager.test_connection(conn_id)
            print(f"  {conn_id}: {'✓ Healthy' if is_healthy else '✗ Failed'}")
        
        # Perform cross-source analysis
        print(f"\nCross-source data analysis:")
        
        # Get data from each source
        results = {}
        for conn_id in active_connections:
            connector = manager.get_connection(conn_id)
            if connector:
                try:
                    tables = connector.list_tables()
                    if tables:
                        sample_data = connector.get_sample_data(tables[0], limit=5)
                        results[conn_id] = {
                            'tables': len(tables),
                            'sample_shape': sample_data.shape,
                            'columns': list(sample_data.columns)
                        }
                except Exception as e:
                    results[conn_id] = {'error': str(e)}
        
        # Display results
        for conn_id, data in results.items():
            if 'error' in data:
                print(f"  {conn_id}: Error - {data['error']}")
            else:
                print(f"  {conn_id}: {data['tables']} tables, "
                      f"sample shape {data['sample_shape']}, "
                      f"columns: {data['columns'][:3]}...")
        
        # Get connection metrics
        metrics = manager.get_connection_metrics()
        print(f"\nConnection metrics available for {len(metrics)} connections")
        
    finally:
        manager.close_all()
        import shutil
        shutil.rmtree(temp_dir)


def example_performance_optimization():
    """Example: Performance optimization and monitoring."""
    print("\n=== Example: Performance Optimization ===")
    
    # Create sample files with more data for performance testing
    temp_dir, parquet_dir, csv_dir = create_sample_files()
    
    try:
        metrics_collector = get_metrics_collector()
        
        # Test different connection methods and compare performance
        parquet_config = ConnectionConfig(host=str(parquet_dir))
        
        print("Testing Parquet connector performance:")
        
        # Test multiple connection attempts to gather metrics
        for i in range(3):
            try:
                connector = ConnectorFactory.create_connector('parquet', parquet_config)
                
                start_time = time.time()
                connector.connect()
                connect_time = time.time() - start_time
                
                # Get a table list
                tables = connector.list_tables()
                if tables:
                    # Read some data
                    start_query = time.time()
                    sample = connector.get_sample_data(tables[0], limit=100)
                    query_time = time.time() - start_query
                    
                    print(f"  Run {i+1}: Connect: {connect_time:.3f}s, "
                          f"Query: {query_time:.3f}s, Rows: {len(sample)}")
                
                connector.close()
                
            except Exception as e:
                print(f"  Run {i+1}: Failed - {e}")
        
        # Get performance statistics
        stats = metrics_collector.get_connection_statistics(source_type='parquet')
        if stats:
            print(f"\nPerformance Statistics:")
            for key, performance in stats.items():
                print(f"  {key}:")
                print(f"    Success rate: {performance.success_rate:.1%}")
                print(f"    Avg duration: {performance.avg_duration:.3f}s")
                print(f"    Min/Max duration: {performance.min_duration:.3f}s / {performance.max_duration:.3f}s")
        
        # Compare methods for the same source (if multiple methods available)
        comparison = metrics_collector.get_method_comparison('parquet')
        if comparison:
            print(f"\nMethod Comparison:")
            for method, data in comparison.items():
                stats = data['performance_stats']
                print(f"  {method}:")
                print(f"    Usage: {data['usage_frequency']} times")
                print(f"    Avg time: {stats.avg_duration:.3f}s")
                print(f"    Success rate: {stats.success_rate:.1%}")
        
        # Show query performance
        query_stats = metrics_collector.get_query_statistics()
        print(f"\nQuery Statistics:")
        print(f"  Total queries: {query_stats['total_queries']}")
        print(f"  Success rate: {query_stats['success_rate']:.1%}")
        print(f"  Avg execution time: {query_stats['avg_execution_time']:.3f}s")
    
    finally:
        import shutil
        shutil.rmtree(temp_dir)


def example_error_handling_and_fallback():
    """Example: Error handling and connection method fallback."""
    print("\n=== Example: Error Handling and Fallback ===")
    
    # Create a connector with intentionally problematic configuration
    print("Testing fallback mechanisms:")
    
    # Create sample files first
    temp_dir, parquet_dir, csv_dir = create_sample_files()
    
    try:
        # Test with non-existent directory (should fail)
        bad_config = ConnectionConfig(host='/nonexistent/path')
        
        try:
            connector = ConnectorFactory.create_connector('csv', bad_config)
            connector.connect()
            print("✗ Unexpected success with bad path")
        except Exception as e:
            print(f"✓ Expected failure with bad path: {e}")
            
            # Check what methods were attempted
            metrics = connector.get_connection_metrics()
            print(f"  Connection attempts: {len(metrics)}")
            for metric in metrics:
                status = "✓" if metric['success'] else "✗"
                print(f"    {status} {metric['method']}: {metric.get('error_message', 'Success')}")
        
        # Test with mixed valid/invalid files
        print(f"\nTesting with mixed file types:")
        
        # Add an invalid file to the directory
        invalid_file = Path(csv_dir) / 'invalid.txt'
        with open(invalid_file, 'w') as f:
            f.write("This is not a CSV file\nIt has invalid content\n")
        
        mixed_config = ConnectionConfig(host=str(csv_dir))
        connector = ConnectorFactory.create_connector('csv', mixed_config)
        
        try:
            connector.connect()
            tables = connector.list_tables()
            print(f"  Found {len(tables)} files (including invalid ones)")
            
            # Try to read data - should handle errors gracefully
            for table in tables[:2]:  # Test first 2 tables
                try:
                    sample = connector.get_sample_data(table, limit=3)
                    print(f"  ✓ {table}: {sample.shape[0]} rows")
                except Exception as e:
                    print(f"  ✗ {table}: {e}")
        
        except Exception as e:
            print(f"✗ Connection failed: {e}")
        
        # Test recovery after errors
        print(f"\nTesting recovery mechanisms:")
        
        good_config = ConnectionConfig(host=str(parquet_dir))
        connector = ConnectorFactory.create_connector('parquet', good_config)
        
        try:
            # First successful connection
            connector.connect()
            print("  ✓ Initial connection successful")
            
            # Simulate connection loss
            connector.connection = None
            
            # Try to use - should auto-reconnect
            tables = connector.list_tables()
            print(f"  ✓ Auto-reconnected and found {len(tables)} tables")
            
        except Exception as e:
            print(f"  ✗ Recovery failed: {e}")
        
    finally:
        import shutil
        shutil.rmtree(temp_dir)


def example_type_mapping_advanced():
    """Example: Advanced type mapping and data conversion."""
    print("\n=== Example: Advanced Type Mapping ===")
    
    from diffforge.core.types import DataTypeMapper
    
    # Show comprehensive type mappings
    sources = DataTypeMapper.get_supported_sources()
    print(f"Supported sources: {len(sources)}")
    
    # Test type mappings across different sources
    test_cases = [
        # (source, native_type, expected_common_type)
        ('snowflake', 'VARCHAR(255)', 'string'),
        ('snowflake', 'NUMBER(10,2)', 'decimal'),
        ('snowflake', 'TIMESTAMP_NTZ', 'timestamp'),
        ('postgresql', 'varchar(100)', 'string'),
        ('postgresql', 'bigint', 'int64'),
        ('postgresql', 'jsonb', 'json'),
        ('mysql', 'VARCHAR(255)', 'string'),
        ('mysql', 'DECIMAL(10,2)', 'decimal'),
        ('mysql', 'DATETIME', 'datetime'),
        ('bigquery', 'STRING', 'string'),
        ('bigquery', 'NUMERIC', 'decimal'),
        ('bigquery', 'TIMESTAMP', 'timestamp'),
        ('databricks', 'STRING', 'string'),
        ('databricks', 'DECIMAL(10,2)', 'decimal'),
        ('databricks', 'TIMESTAMP', 'timestamp'),
    ]
    
    print(f"\nType Mapping Examples:")
    for source, native_type, expected in test_cases:
        try:
            mapped = DataTypeMapper.map_type(source, native_type)
            status = "✓" if mapped == expected else f"✗ (got {mapped})"
            print(f"  {source}.{native_type} → {mapped} {status}")
        except Exception as e:
            print(f"  {source}.{native_type} → Error: {e}")
    
    # Show source-specific types
    print(f"\nSource-specific type inventories:")
    for source in sorted(sources):
        types = DataTypeMapper.get_source_types(source)
        print(f"  {source}: {len(types)} native types")
        # Show first few types as examples
        sample_types = sorted(types)[:5]
        print(f"    Examples: {', '.join(sample_types)}")


def main():
    """Run all advanced examples."""
    print("DiffForge Advanced Usage Examples")
    print("=" * 50)
    
    import time
    globals()['time'] = time  # Make time available for performance example
    
    # Run examples
    example_all_connector_types()
    example_advanced_connection_management()
    example_performance_optimization()
    example_error_handling_and_fallback()
    example_type_mapping_advanced()
    
    print(f"\n{'=' * 50}")
    print("Advanced examples completed!")
    print("\nFor real database connections, ensure you have:")
    print("- Proper credentials and network access")
    print("- Required driver packages installed")
    print("- Environment variables configured")


if __name__ == '__main__':
    main()