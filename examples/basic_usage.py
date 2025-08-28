#!/usr/bin/env python3
"""
Basic usage examples for DiffForge connectors.

This script demonstrates how to use the DiffForge connector system
to connect to different data sources, retrieve metadata, and sample data.
"""

import os
import pandas as pd
from typing import Dict, Any

from diffforge.core.models import ConnectionConfig
from diffforge.core.factory import ConnectorFactory
from diffforge.core.manager import ConnectionManager
from diffforge.metrics.collector import get_metrics_collector


def create_snowflake_config() -> ConnectionConfig:
    """Create Snowflake connection configuration from environment variables."""
    return ConnectionConfig(
        host=os.getenv('SNOWFLAKE_ACCOUNT', 'your-account.snowflakecomputing.com'),
        username=os.getenv('SNOWFLAKE_USER', 'your_username'),
        password=os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'YOUR_DATABASE'),
        schema_name=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        extra_params={
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'PUBLIC'),
        }
    )


def create_postgresql_config() -> ConnectionConfig:
    """Create PostgreSQL connection configuration from environment variables."""
    return ConnectionConfig(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        username=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'password'),
        database=os.getenv('POSTGRES_DATABASE', 'postgres'),
        schema_name=os.getenv('POSTGRES_SCHEMA', 'public'),
        extra_params={
            'ssl_mode': os.getenv('POSTGRES_SSL_MODE', 'prefer'),
        }
    )


def example_direct_connector_usage():
    """Example 1: Direct connector usage without connection manager."""
    print("=== Example 1: Direct Connector Usage ===")
    
    try:
        # Create Snowflake connector directly
        config = create_snowflake_config()
        connector = ConnectorFactory.create_connector('snowflake', config)
        
        print(f"Created connector: {connector}")
        print(f"Available connection methods: {[m.value for m in connector.connection_methods]}")
        
        # Test connection
        if connector.test_connection():
            print("✓ Connection test successful")
            
            # List schemas
            schemas = connector.list_schemas()
            print(f"Available schemas: {schemas[:5]}...")  # Show first 5
            
            # List tables in first schema
            if schemas:
                tables = connector.list_tables(schemas[0])
                print(f"Tables in {schemas[0]}: {tables[:5]}...")  # Show first 5
                
                # Get metadata for first table
                if tables:
                    metadata = connector.get_table_metadata(tables[0], schemas[0])
                    print(f"Table {tables[0]} has {len(metadata.columns)} columns")
                    print(f"Row count: {metadata.row_count}")
                    
                    # Get sample data
                    sample = connector.get_sample_data(tables[0], limit=3, schema_name=schemas[0])
                    print(f"Sample data:\n{sample}")
            
        else:
            print("✗ Connection test failed")
            
        # Get connection metrics
        metrics = connector.get_connection_metrics()
        print(f"Connection attempts: {len(metrics)}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'connector' in locals():
            connector.close()


def example_connection_manager():
    """Example 2: Using connection manager for multiple connections."""
    print("\n=== Example 2: Connection Manager ===")
    
    manager = ConnectionManager()
    
    try:
        # Create multiple connections
        sf_config = create_snowflake_config()
        pg_config = create_postgresql_config()
        
        # Add Snowflake connection
        try:
            sf_connector = manager.create_connection(
                connection_id='snowflake_prod',
                source_type='snowflake',
                connection_config=sf_config,
                connect_immediately=False  # Don't connect immediately for demo
            )
            print(f"✓ Snowflake connector created: {sf_connector}")
        except Exception as e:
            print(f"✗ Snowflake connection failed: {e}")
        
        # Add PostgreSQL connection
        try:
            pg_connector = manager.create_connection(
                connection_id='postgres_local',
                source_type='postgresql', 
                connection_config=pg_config,
                connect_immediately=False  # Don't connect immediately for demo
            )
            print(f"✓ PostgreSQL connector created: {pg_connector}")
        except Exception as e:
            print(f"✗ PostgreSQL connection failed: {e}")
        
        # List all connections
        connections = manager.list_connections()
        print(f"\nManaged connections:")
        for conn in connections:
            print(f"  - {conn['connection_id']}: {conn['source_type']} ({'connected' if conn['connected'] else 'not connected'})")
        
        # Test connections
        for conn in connections:
            conn_id = conn['connection_id']
            if manager.test_connection(conn_id):
                print(f"✓ {conn_id} connection test successful")
                
                # Get connector and run a simple query
                connector = manager.get_connection(conn_id)
                if connector:
                    try:
                        result = connector.execute_query(connector._get_test_query())
                        print(f"  Test query executed in {result.execution_time:.3f}s")
                    except Exception as e:
                        print(f"  Test query failed: {e}")
            else:
                print(f"✗ {conn_id} connection test failed")
        
        # Get connection metrics
        metrics = manager.get_connection_metrics()
        print(f"\nConnection metrics available for {len(metrics)} connections")
        
    finally:
        manager.close_all()


def example_performance_monitoring():
    """Example 3: Performance monitoring and metrics."""
    print("\n=== Example 3: Performance Monitoring ===")
    
    metrics_collector = get_metrics_collector()
    
    try:
        # Simulate some connection attempts and queries
        print("Simulating connection attempts...")
        
        # Successful Snowflake connections
        metrics_collector.record_connection_attempt(
            source_type='snowflake',
            method='arrow',
            success=True,
            duration=1.2,
            connection_id='sf_conn_1'
        )
        
        metrics_collector.record_connection_attempt(
            source_type='snowflake', 
            method='arrow',
            success=True,
            duration=0.8,
            connection_id='sf_conn_2'
        )
        
        # Failed Snowflake connection
        metrics_collector.record_connection_attempt(
            source_type='snowflake',
            method='native',
            success=False,
            duration=5.0,
            error_message='Authentication failed',
            connection_id='sf_conn_3'
        )
        
        # PostgreSQL connections
        metrics_collector.record_connection_attempt(
            source_type='postgresql',
            method='asyncpg',
            success=True,
            duration=0.5,
            connection_id='pg_conn_1'
        )
        
        # Simulate query executions
        print("Simulating query executions...")
        
        metrics_collector.record_query_execution(
            connection_id='sf_conn_1',
            query='SELECT COUNT(*) FROM large_table',
            execution_time=2.5,
            rows_returned=1,
            bytes_transferred=100,
            connection_method='arrow'
        )
        
        metrics_collector.record_query_execution(
            connection_id='sf_conn_2',
            query='SELECT * FROM users LIMIT 100',
            execution_time=0.3,
            rows_returned=100,
            bytes_transferred=5000,
            connection_method='arrow'
        )
        
        # Get connection statistics
        conn_stats = metrics_collector.get_connection_statistics()
        print("\nConnection Statistics:")
        for key, stats in conn_stats.items():
            print(f"  {key}:")
            print(f"    Success rate: {stats.success_rate:.1%}")
            print(f"    Avg duration: {stats.avg_duration:.2f}s")
            print(f"    Total attempts: {stats.total_attempts}")
        
        # Get query statistics
        query_stats = metrics_collector.get_query_statistics()
        print(f"\nQuery Statistics:")
        print(f"  Total queries: {query_stats['total_queries']}")
        print(f"  Success rate: {query_stats['success_rate']:.1%}")
        print(f"  Avg execution time: {query_stats['avg_execution_time']:.3f}s")
        print(f"  Total rows returned: {query_stats['total_rows']}")
        
        # Compare connection methods for Snowflake
        sf_comparison = metrics_collector.get_method_comparison('snowflake')
        print(f"\nSnowflake Method Comparison:")
        for method, data in sf_comparison.items():
            stats = data['performance_stats']
            print(f"  {method}:")
            print(f"    Usage frequency: {data['usage_frequency']}")
            print(f"    Success rate: {stats.success_rate:.1%}")
            print(f"    Avg duration: {stats.avg_duration:.2f}s")
        
        # Get top errors
        errors = metrics_collector.get_top_errors(limit=3)
        print(f"\nTop Errors:")
        for error, count in errors:
            print(f"  {error}: {count} occurrences")
    
    except Exception as e:
        print(f"Error in performance monitoring: {e}")


def example_error_handling():
    """Example 4: Error handling and connection fallback."""
    print("\n=== Example 4: Error Handling and Fallback ===")
    
    # Create a config with invalid credentials to test fallback
    config = ConnectionConfig(
        host='invalid-host.example.com',
        username='invalid_user',
        password='invalid_password',
        database='invalid_db'
    )
    
    try:
        connector = ConnectorFactory.create_connector('postgresql', config)
        
        print(f"Attempting connection with {len(connector.connection_methods)} methods...")
        print(f"Methods to try: {[m.value for m in connector.connection_methods]}")
        
        # This will try all connection methods and fail
        connector.connect()
        
    except Exception as e:
        print(f"✓ Expected failure: {e}")
        
        # Get metrics to see which methods were tried
        if 'connector' in locals():
            metrics = connector.get_connection_metrics()
            print(f"Connection attempts made: {len(metrics)}")
            for metric in metrics:
                status = "✓" if metric['success'] else "✗"
                print(f"  {status} {metric['method']}: {metric.get('error_message', 'Success')}")


def example_type_mapping():
    """Example 5: Type system and data type mapping."""
    print("\n=== Example 5: Type System ===")
    
    from diffforge.core.types import DataTypeMapper
    
    # Show supported sources
    print(f"Supported sources: {DataTypeMapper.get_supported_sources()}")
    print(f"Common types: {DataTypeMapper.get_common_types()}")
    
    # Test type mappings
    test_types = [
        ('snowflake', 'VARCHAR(255)'),
        ('snowflake', 'NUMBER(10,2)'),
        ('snowflake', 'TIMESTAMP'),
        ('postgresql', 'varchar'),
        ('postgresql', 'integer'),
        ('postgresql', 'jsonb'),
        ('mysql', 'VARCHAR'),
        ('mysql', 'DECIMAL'),
        ('bigquery', 'STRING'),
        ('bigquery', 'TIMESTAMP'),
    ]
    
    print(f"\nType Mapping Examples:")
    for source, native_type in test_types:
        try:
            common_type = DataTypeMapper.map_type(source, native_type)
            print(f"  {source}.{native_type} → {common_type}")
        except Exception as e:
            print(f"  {source}.{native_type} → Error: {e}")


def main():
    """Run all examples."""
    print("DiffForge Connector Examples")
    print("=" * 50)
    
    # Run examples
    example_direct_connector_usage()
    example_connection_manager()
    example_performance_monitoring()
    example_error_handling()
    example_type_mapping()
    
    print(f"\n{'=' * 50}")
    print("Examples completed!")
    print("\nTo run with real connections, set environment variables:")
    print("  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.")
    print("  POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, etc.")


if __name__ == '__main__':
    main()