#!/usr/bin/env python3
"""
DiffForge Snowflake Connector Test Script

This script performs comprehensive testing of the Snowflake connector including:
1. Code quality checks (formatting, linting, type checking)
2. Unit tests for mock-based testing
3. Integration tests with real Snowflake connection
4. Performance benchmarking

Usage:
    python test_snowflake.py [--unit-only] [--integration-only] [--skip-quality]

Requirements:
    - .env file with Snowflake credentials
    - Dependencies installed: pip install -e ".[snowflake,dev]"
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv

from diffforge.connectors.snowflake.connector import SnowflakeConnector
from diffforge.core.factory import ConnectorFactory
from diffforge.core.models import ConnectionConfig, ConnectionMethod


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(title: str) -> None:
    """Print a formatted header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{title.center(60)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.END}\n")


def print_success(message: str) -> None:
    """Print success message"""
    print(f"{Colors.GREEN}✅ {message}{Colors.END}")


def print_error(message: str) -> None:
    """Print error message"""
    print(f"{Colors.RED}❌ {message}{Colors.END}")


def print_warning(message: str) -> None:
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.END}")


def print_info(message: str) -> None:
    """Print info message"""
    print(f"{Colors.CYAN}ℹ️  {message}{Colors.END}")


def run_command(cmd: List[str], description: str, check: bool = True) -> bool:
    """Run a shell command and return success status"""
    print(f"{Colors.YELLOW}Running: {description}{Colors.END}")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
        if result.returncode == 0:
            print_success(f"{description} completed successfully")
            if result.stdout.strip():
                print(f"Output:\n{result.stdout}")
            return True
        else:
            print_error(f"{description} failed")
            if result.stderr.strip():
                print(f"Error:\n{result.stderr}")
            return False
    except subprocess.CalledProcessError as e:
        print_error(f"{description} failed with return code {e.returncode}")
        if e.stderr:
            print(f"Error:\n{e.stderr}")
        return False
    except FileNotFoundError:
        print_error(f"Command not found: {cmd[0]}")
        return False


def check_dependencies() -> bool:
    """Check if required dependencies are installed"""
    print_header("DEPENDENCY CHECK")
    
    try:
        import snowflake.connector
        print_success("snowflake-connector-python is installed")
    except ImportError:
        print_error("snowflake-connector-python is not installed")
        print_info("Run: pip install 'snowflake-connector-python[pandas,secure-local-storage]>=3.0.0'")
        return False
    
    try:
        import pytest
        print_success("pytest is installed")
    except ImportError:
        print_error("pytest is not installed")
        print_info("Run: pip install -e \".[dev]\"")
        return False
    
    try:
        import black
        print_success("black is installed")
    except ImportError:
        print_warning("black is not installed (code formatting will be skipped)")
    
    try:
        import isort
        print_success("isort is installed")
    except ImportError:
        print_warning("isort is not installed (import sorting will be skipped)")
    
    return True


def check_environment() -> Optional[ConnectionConfig]:
    """Check environment variables and create connection config"""
    print_header("ENVIRONMENT CHECK")
    
    # Load environment variables
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv()
        print_success("Loaded .env file")
    else:
        print_warning("No .env file found")
    
    # Check required environment variables
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE',
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print_error(f"Missing required environment variables: {missing_vars}")
        print_info("Please set these variables in your .env file")
        return None
    
    # Create connection config
    config = ConnectionConfig(
        host=os.getenv('SNOWFLAKE_ACCOUNT'),
        username=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'TESTDB'),
        schema_name=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        extra_params={
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            'authenticator': os.getenv('SNOWFLAKE_AUTHENTICATOR', 'snowflake'),
        }
    )
    
    print_success("Environment configuration loaded:")
    print(f"  Account: {config.host}")
    print(f"  User: {config.username}")
    print(f"  Database: {config.database}")
    print(f"  Schema: {config.schema_name}")
    print(f"  Warehouse: {config.extra_params.get('warehouse')}")
    print(f"  Role: {config.extra_params.get('role')}")
    
    return config


def run_code_quality_checks() -> bool:
    """Run code quality checks"""
    print_header("CODE QUALITY CHECKS")
    
    success = True
    
    # Black formatting
    if run_command(
        ["black", "--check", "diffforge/", "tests/", "--line-length", "88"],
        "Code formatting check with Black",
        check=False
    ):
        print_success("Code is properly formatted")
    else:
        print_warning("Code formatting issues found. Run: black diffforge/ tests/ --line-length 88")
        success = False
    
    # Import sorting
    if run_command(
        ["isort", "diffforge/", "tests/", "--check-only"],
        "Import sorting check with isort",
        check=False
    ):
        print_success("Imports are properly sorted")
    else:
        print_warning("Import sorting issues found. Run: isort diffforge/ tests/")
        success = False
    
    # Linting with flake8
    if run_command(
        ["flake8", "diffforge/", "--max-line-length=88", "--extend-ignore=E203,W503"],
        "Linting check with flake8",
        check=False
    ):
        print_success("No linting issues found")
    else:
        print_warning("Linting issues found")
        success = False
    
    # Type checking (optional - don't fail on this)
    run_command(
        ["mypy", "diffforge/", "--no-strict-optional", "--ignore-missing-imports"],
        "Type checking with mypy",
        check=False
    )
    
    return success


def run_unit_tests() -> bool:
    """Run unit tests"""
    print_header("UNIT TESTS")
    
    return run_command(
        ["pytest", "tests/test_connectors.py::TestSnowflakeConnector", "-v", "--tb=short"],
        "Snowflake unit tests",
        check=False
    )


def run_integration_tests(config: ConnectionConfig) -> bool:
    """Run integration tests with real Snowflake connection"""
    print_header("INTEGRATION TESTS")
    
    return run_command(
        ["pytest", "tests/test_snowflake_integration.py", "-v", "-s", "--tb=short"],
        "Snowflake integration tests",
        check=False
    )


def run_manual_connection_test(config: ConnectionConfig) -> bool:
    """Run manual connection test and show detailed results"""
    print_header("MANUAL CONNECTION TEST")
    
    print_info("Testing connection to Snowflake...")
    
    try:
        # Test connection
        with SnowflakeConnector(config) as connector:
            start_time = time.time()
            connection_time = time.time() - start_time
            
            print_success(f"Connected successfully using {connector.connection_method_used.value}")
            print_info(f"Connection time: {connection_time:.3f} seconds")
            
            # Test basic query
            result = connector.execute_query("SELECT CURRENT_VERSION() as version, CURRENT_USER() as user")
            df = result.data
            print_success("Basic query executed successfully")
            print(f"Snowflake version: {df.iloc[0]['VERSION']}")
            print(f"Connected as user: {df.iloc[0]['USER']}")
            
            # List schemas
            schemas = connector.list_schemas()
            print_success(f"Found {len(schemas)} schemas: {schemas}")
            
            # List tables in PUBLIC schema
            tables = connector.list_tables(schema_name='PUBLIC')
            print_success(f"Found {len(tables)} tables in PUBLIC schema: {tables}")
            
            # Check if TBL table exists
            if 'TBL' in tables:
                print_success("Target table 'TBL' found!")
                
                # Get table metadata
                metadata = connector.get_table_metadata('TBL', schema_name='PUBLIC')
                print_info(f"Table metadata:")
                print(f"  Columns: {len(metadata.columns)}")
                print(f"  Row count: {metadata.row_count}")
                
                for col in metadata.columns:
                    print(f"    - {col.name}: {col.data_type} (nullable: {col.nullable})")
                
                # Get sample data
                sample_df = connector.get_sample_data('TBL', limit=5, schema_name='PUBLIC')
                print_info(f"Sample data: {len(sample_df)} rows")
                if not sample_df.empty:
                    print(sample_df.to_string())
                else:
                    print_info("Table is empty")
                
                # Test count query
                count_result = connector.execute_query('SELECT COUNT(*) as total FROM "PUBLIC"."TBL"')
                total_rows = count_result.data.iloc[0]['TOTAL']
                print_info(f"Total rows in TBL: {total_rows}")
                print_info(f"Query execution time: {count_result.execution_time:.3f} seconds")
                
            else:
                print_warning("Target table 'TBL' not found in PUBLIC schema")
                print_info(f"Available tables: {tables}")
            
            # Test connection metrics
            metrics = connector.get_connection_metrics()
            print_info(f"Connection metrics collected: {len(metrics)} entries")
            
            for metric in metrics:
                if metric.get('success'):
                    print_success(f"  {metric['method']}: {metric['duration']:.3f}s")
                else:
                    print_error(f"  {metric['method']}: {metric.get('error_message', 'Failed')}")
        
        return True
        
    except Exception as e:
        print_error(f"Connection test failed: {e}")
        return False


def benchmark_connection_methods(config: ConnectionConfig) -> None:
    """Benchmark different connection methods"""
    print_header("CONNECTION METHOD BENCHMARK")
    
    methods_to_test = [
        ConnectionMethod.ARROW,
        ConnectionMethod.NATIVE,
    ]
    
    results = []
    
    for method in methods_to_test:
        print_info(f"Testing {method.value} connection method...")
        
        try:
            connector = SnowflakeConnector(config)
            
            start_time = time.time()
            
            # Connect using specific method
            if method == ConnectionMethod.ARROW:
                connection = connector._connect_arrow()
            elif method == ConnectionMethod.NATIVE:
                connection = connector._connect_native()
            else:
                continue
            
            connect_time = time.time() - start_time
            
            connector.connection = connection
            connector.connection_method_used = method
            
            # Test query performance
            start_time = time.time()
            result = connector.execute_query("SELECT CURRENT_VERSION()")
            query_time = time.time() - start_time
            
            connector.close()
            
            results.append({
                'method': method.value,
                'connect_time': connect_time,
                'query_time': query_time,
                'success': True
            })
            
            print_success(f"{method.value}: Connect {connect_time:.3f}s, Query {query_time:.3f}s")
            
        except Exception as e:
            results.append({
                'method': method.value,
                'connect_time': 0,
                'query_time': 0,
                'success': False,
                'error': str(e)
            })
            print_error(f"{method.value}: {e}")
    
    # Summary
    print_info("\nBenchmark Summary:")
    successful_methods = [r for r in results if r['success']]
    
    if successful_methods:
        fastest_connect = min(successful_methods, key=lambda x: x['connect_time'])
        fastest_query = min(successful_methods, key=lambda x: x['query_time'])
        
        print_success(f"Fastest connection: {fastest_connect['method']} ({fastest_connect['connect_time']:.3f}s)")
        print_success(f"Fastest query: {fastest_query['method']} ({fastest_query['query_time']:.3f}s)")
    else:
        print_error("No connection methods succeeded")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="DiffForge Snowflake Connector Test Script")
    parser.add_argument("--unit-only", action="store_true", help="Run only unit tests")
    parser.add_argument("--integration-only", action="store_true", help="Run only integration tests")
    parser.add_argument("--skip-quality", action="store_true", help="Skip code quality checks")
    parser.add_argument("--benchmark", action="store_true", help="Run connection method benchmark")
    args = parser.parse_args()
    
    print_header("DIFFFORGE SNOWFLAKE CONNECTOR TEST SCRIPT")
    
    # Check dependencies
    if not check_dependencies():
        print_error("Dependency check failed. Please install required packages.")
        sys.exit(1)
    
    # Check environment
    config = check_environment()
    if not config and not args.unit_only:
        print_error("Environment check failed. Cannot run integration tests.")
        if not args.unit_only:
            sys.exit(1)
    
    success_count = 0
    total_tests = 0
    
    # Code quality checks
    if not args.skip_quality and not args.integration_only:
        total_tests += 1
        if run_code_quality_checks():
            success_count += 1
    
    # Unit tests
    if not args.integration_only:
        total_tests += 1
        if run_unit_tests():
            success_count += 1
    
    # Integration tests
    if not args.unit_only and config:
        total_tests += 1
        if run_integration_tests(config):
            success_count += 1
        
        # Manual connection test
        total_tests += 1
        if run_manual_connection_test(config):
            success_count += 1
        
        # Benchmark if requested
        if args.benchmark:
            benchmark_connection_methods(config)
    
    # Final summary
    print_header("TEST SUMMARY")
    print(f"Tests passed: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        print_success("All tests passed! 🎉")
        sys.exit(0)
    else:
        print_error(f"{total_tests - success_count} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()