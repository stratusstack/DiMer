#!/usr/bin/env python3
"""
Test script for comparing Snowflake tables using DiffForge enhanced compare.py.

This script tests:
1. Tables within the same Snowflake database 
2. Tables across different Snowflake databases

Snowflake configurations are provided via user input.
"""

import sys
import json
import getpass
from typing import Dict, Any

from diffforge.core.models import ConnectionConfig
from diffforge.core.factory import ConnectorFactory
from diffforge.core.compare import Diffcheck


def get_user_input() -> Dict[str, Any]:
    """Get Snowflake configuration from user input."""
    print("=== Snowflake Table Comparison Test ===\n")
    
    # Get comparison type
    print("Select comparison type:")
    print("1. Same database (different tables)")
    print("2. Different databases (cross-database)")
    print("3. JSON configuration")
    
    while True:
        choice = input("Enter your choice (1, 2, or 3): ").strip()
        if choice in ['1', '2', '3']:
            break
        print("Please enter 1, 2, or 3")
    
    if choice == '3':
        return get_json_input()
    
    same_database = choice == '1'
    
    # Get first Snowflake configuration
    print(f"\n--- {'Database' if same_database else 'First Database'} Configuration ---")
    config1 = get_snowflake_config("Database 1" if not same_database else "Database")
    
    # Get table information for first database
    table1 = input("Enter first table name: ").strip()
    keys1 = input("Enter primary key columns for first table (comma-separated): ").strip().split(',')
    keys1 = [key.strip() for key in keys1]
    
    if same_database:
        # Same database - reuse config
        config2 = config1
        table2 = input("Enter second table name: ").strip()
    else:
        # Different database
        print(f"\n--- Second Database Configuration ---")
        config2 = get_snowflake_config("Database 2")
        table2 = input("Enter second table name: ").strip()
    
    keys2 = input("Enter primary key columns for second table (comma-separated): ").strip().split(',')
    keys2 = [key.strip() for key in keys2]
    
    return {
        'same_database': same_database,
        'config1': config1,
        'config2': config2,
        'table1': table1,
        'table2': table2,
        'keys1': keys1,
        'keys2': keys2
    }


def get_snowflake_config(label: str) -> ConnectionConfig:
    """Get Snowflake configuration for a single database."""
    print(f"\nEnter {label} connection details:")
    
    host = input("Snowflake Account (e.g., myaccount.snowflakecomputing.com): ").strip()
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    database = input("Database: ").strip()
    schema = input("Schema (default: PUBLIC): ").strip() or "PUBLIC"
    warehouse = input("Warehouse (default: COMPUTE_WH): ").strip() or "COMPUTE_WH"
    role = input("Role (default: PUBLIC): ").strip() or "PUBLIC"
    
    return ConnectionConfig(
        host=host,
        username=username,
        password=password,
        database=database,
        schema_name=schema,
        extra_params={
            'warehouse': warehouse,
            'role': role,
            'authenticator': 'snowflake'
        }
    )


def get_json_input() -> Dict[str, Any]:
    """Get configuration from JSON input."""
    print("\n=== JSON Configuration ===")
    print("Paste your JSON configuration (press Ctrl+D or Ctrl+Z when done):")
    
    try:
        # Read multi-line JSON input
        json_lines = []
        while True:
            try:
                line = input()
                json_lines.append(line)
            except EOFError:
                break
        
        json_string = '\n'.join(json_lines)
        config_data = json.loads(json_string)
        
        # Validate required keys
        if 'database1' not in config_data or 'database2' not in config_data:
            raise ValueError("JSON must contain 'database1' and 'database2' keys")
        
        # Extract database configurations
        db1_data = config_data['database1']
        db2_data = config_data['database2']
        
        # Create ConnectionConfig objects
        config1 = ConnectionConfig(
            host=db1_data['config']['host'],
            username=db1_data['config']['username'],
            password=db1_data['config']['password'],
            database=db1_data['config']['database'],
            schema_name=db1_data['config']['schema_name'],
            extra_params=db1_data['config'].get('extra_params', {})
        )
        
        config2 = ConnectionConfig(
            host=db2_data['config']['host'],
            username=db2_data['config']['username'],
            password=db2_data['config']['password'],
            database=db2_data['config']['database'],
            schema_name=db2_data['config']['schema_name'],
            extra_params=db2_data['config'].get('extra_params', {})
        )
        
        # Determine if same database based on type and host
        same_database = (db1_data['type'] == db2_data['type'] and 
                        db1_data['config']['host'] == db2_data['config']['host'] and
                        db1_data['config']['database'] == db2_data['config']['database'])
        
        # Extract table names and keys
        table1 = db1_data.get('fq_table_name', db1_data.get('table_name', ''))
        table2 = db2_data.get('fq_table_name', db2_data.get('table_name', ''))
        keys1 = db1_data.get('keys', [])
        keys2 = db2_data.get('keys', [])
        
        return {
            'same_database': same_database,
            'config1': config1,
            'config2': config2,
            'table1': table1,
            'table2': table2,
            'keys1': keys1,
            'keys2': keys2,
            'json_mode': True
        }
        
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON format: {e}")
        raise
    except KeyError as e:
        print(f"✗ Missing required key in JSON: {e}")
        raise
    except Exception as e:
        print(f"✗ Error processing JSON configuration: {e}")
        raise


def build_fully_qualified_table_names(user_input: Dict[str, Any], same_database: bool = True) -> tuple[str, str]:
    """Build fully qualified table names handling JSON vs manual input."""
    json_mode = user_input.get('json_mode', False)
    
    if json_mode and ('.' in user_input['table1'] or '"' in user_input['table1']):
        # JSON mode with already qualified table names
        return user_input['table1'], user_input['table2']
    else:
        # Manual input or simple table names - construct FQ names
        if same_database:
            schema = user_input['config1'].schema_name
            fq_table1 = f'"{schema}"."{user_input["table1"]}"'
            fq_table2 = f'"{schema}"."{user_input["table2"]}"'
        else:
            schema1 = user_input['config1'].schema_name
            schema2 = user_input['config2'].schema_name
            fq_table1 = f'"{schema1}"."{user_input["table1"]}"'
            fq_table2 = f'"{schema2}"."{user_input["table2"]}"'
        return fq_table1, fq_table2


def create_db_configs(user_input: Dict[str, Any], fq_table1: str, fq_table2: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Create database configuration dictionaries for Diffcheck."""
    db1_config = {
        'keys': user_input['keys1'],
        'fq_table_name': fq_table1
    }
    
    db2_config = {
        'keys': user_input['keys2'],
        'fq_table_name': fq_table2
    }
    
    return db1_config, db2_config


def print_connection_metrics(connector, label: str = "Connection"):
    """Print connection metrics for a connector."""
    metrics = connector.get_connection_metrics()
    print(f"{label} attempts: {len(metrics)}")
    for i, metric in enumerate(metrics):
        status = "✓" if metric['success'] else "✗"
        print(f"  Attempt {i+1}: {status} {metric['method']} ({metric['duration']:.2f}s)")



def create_connector(config: ConnectionConfig, label: str):
    """Create and test a Snowflake connector."""
    try:
        print(f"\nCreating {label} connector...")
        connector = ConnectorFactory.create_connector('snowflake', config)
        
        print(f"Testing {label} connection...")
        if not connector.test_connection():
            print(f"✗ Connection test failed for {label}")
            return None
        
        print(f"✓ {label} connection successful")
        print(f"  Connection method: {connector.connection_method_used.value}")
        return connector
        
    except Exception as e:
        print(f"✗ Error creating {label} connector: {e}")
        return None


def test_same_database_comparison(user_input: Dict[str, Any]):
    """Test comparison of tables within the same database."""
    print(f"\n=== SAME DATABASE COMPARISON ===")
    print(f"Database: {user_input['config1'].database}")
    print(f"Schema: {user_input['config1'].schema_name}")
    print(f"Comparing: {user_input['table1']} vs {user_input['table2']}")
    
    connector = create_connector(user_input['config1'], "Database")
    if not connector:
        return
    
    try:
        # Build fully qualified table names
        fq_table1, fq_table2 = build_fully_qualified_table_names(user_input, same_database=True)
        
        # Prepare database configurations for Diffcheck
        db1_config, db2_config = create_db_configs(user_input, fq_table1, fq_table2)
        
        # Create Diffcheck instance (same connector for both tables)
        diff_checker = Diffcheck(connector, connector, db1_config, db2_config)
        
        # Perform schema comparison first
        print(f"\n--- Schema Comparison ---")
        schema_compatible = diff_checker.check_schema(fq_table1, fq_table2)
        print(f"Schema compatibility: {'✓' if schema_compatible else '✗'}")

        
        # Perform data comparison
        print(f"\n--- Data Comparison ---")
        diff_checker.compare_within_database("JOIN_DIFF")
        
        # Get connection metrics
        print(f"\n--- Performance Metrics ---")
        print_connection_metrics(connector, "Connection")
        
    except Exception as e:
        print(f"✗ Error during same database comparison: {e}")
    finally:
        connector.close()


def test_cross_database_comparison(user_input: Dict[str, Any]):
    """Test comparison of tables across different databases."""
    print(f"\n=== CROSS-DATABASE COMPARISON ===")
    print(f"Database 1: {user_input['config1'].database} (Table: {user_input['table1']})")
    print(f"Database 2: {user_input['config2'].database} (Table: {user_input['table2']})")
    
    connector1 = create_connector(user_input['config1'], "Database 1")
    connector2 = create_connector(user_input['config2'], "Database 2")
    
    if not connector1 or not connector2:
        if connector1:
            connector1.close()
        if connector2:
            connector2.close()
        return
    
    try:
        # Build fully qualified table names
        fq_table1, fq_table2 = build_fully_qualified_table_names(user_input, same_database=False)
        
        # Prepare database configurations for Diffcheck
        db1_config, db2_config = create_db_configs(user_input, fq_table1, fq_table2)
        
        # Create Diffcheck instance
        diff_checker = Diffcheck(connector1, connector2, db1_config, db2_config)
        
        # Perform schema comparison first
        print(f"\n--- Schema Comparison ---")
        schema_compatible = diff_checker.check_schema(fq_table1, fq_table2)
        print(f"Schema compatibility: {'✓' if schema_compatible else '✗'}")
        
        # Perform cross-database comparison
        print(f"\n--- Cross-Database Data Comparison ---")
        diff_checker.compare_cross_database("JOIN_DIFF")
        
        # Get connection metrics for both connectors
        print(f"\n--- Performance Metrics ---")
        print_connection_metrics(connector1, "Database 1 connection")
        print_connection_metrics(connector2, "Database 2 connection")
        
    except Exception as e:
        print(f"✗ Error during cross-database comparison: {e}")
    finally:
        connector1.close()
        connector2.close()



def main():
    """Main function to run table comparison tests."""
    try:
        # Get user input
        user_input = get_user_input()
        
        # Show configuration summary
        print(f"\n=== Configuration Summary ===")
        json_mode = user_input.get('json_mode', False)
        print(f"Input mode: {'JSON Configuration' if json_mode else 'Manual Input'}")
        
        if user_input['same_database']:
            print(f"Type: Same database comparison")
            print(f"Database: {user_input['config1'].database}")
            print(f"Schema: {user_input['config1'].schema_name}")
            print(f"Tables: {user_input['table1']} vs {user_input['table2']}")
        else:
            print(f"Type: Cross-database comparison")
            print(f"Database 1: {user_input['config1'].database} (Table: {user_input['table1']})")
            print(f"Database 2: {user_input['config2'].database} (Table: {user_input['table2']})")
        
        print(f"Primary keys: {user_input['keys1']} vs {user_input['keys2']}")
        
        # Confirm before proceeding
        confirm = input(f"\nProceed with comparison? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Comparison cancelled.")
            return 0
        
        # Run appropriate test
        if user_input['same_database']:
            test_same_database_comparison(user_input)
        else:
            test_cross_database_comparison(user_input)
        
        print(f"\n✓ Table comparison test completed!")
        return 0
        
    except KeyboardInterrupt:
        print(f"\n\nComparison interrupted by user.")
        return 1
    except Exception as e:
        print(f"\n✗ Error during table comparison: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)