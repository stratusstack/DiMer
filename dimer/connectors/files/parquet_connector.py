"""Parquet file connector with multiple access strategies."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from dimer.core.base import DataSourceConnector
from dimer.core.models import (
    ColumnMetadata,
    ConnectionConfig,
    ConnectionMethod,
    TableMetadata,
)
from dimer.core.types import DataTypeMapper

logger = structlog.get_logger(__name__)


class ParquetConnector(DataSourceConnector):
    """Parquet file connector with multiple access strategies."""

    # Parquet-specific configuration
    DEFAULT_ENGINE = "pyarrow"
    SUPPORTED_EXTENSIONS = [".parquet", ".pq"]

    def get_required_params(self) -> List[str]:
        """Return list of required connection parameters for Parquet files."""
        # For file-based connectors, we use 'host' as the base directory path
        return ["host"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return methods in order of preference (fastest/most efficient first).

        Returns:
            Ordered list of connection methods to try
        """
        return [
            ConnectionMethod.PYARROW_DIRECT,  # Fastest with PyArrow
            ConnectionMethod.PANDAS_DIRECT,  # Pandas with various engines
        ]

    def _connect_pyarrow_direct(self) -> Any:
        """
        Connect using PyArrow for direct Parquet access.

        Returns:
            PyArrow dataset or file system connection

        Raises:
            ImportError: If PyArrow is not installed
            Exception: If connection fails
        """
        try:
            import pyarrow as pa
            import pyarrow.dataset as ds
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow is required for PyArrow direct connection")

        base_path = self.connection_config.host

        # Validate base path exists
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Base directory not found: {base_path}")

        logger.info("Attempting PyArrow direct connection to Parquet files")

        # Create a dataset for the directory or file
        path = Path(base_path)

        if path.is_file():
            # Single file
            if path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
                raise ValueError(
                    f"File must have one of these extensions: {self.SUPPORTED_EXTENSIONS}"
                )
            dataset = ds.dataset(str(path))
        elif path.is_dir():
            # Directory - scan for parquet files
            try:
                dataset = ds.dataset(str(path), format="parquet")
            except Exception as e:
                # Fallback: manually find parquet files
                parquet_files = list(path.glob("**/*.parquet")) + list(
                    path.glob("**/*.pq")
                )
                if not parquet_files:
                    raise FileNotFoundError(
                        f"No Parquet files found in directory: {base_path}"
                    )
                dataset = ds.dataset([str(f) for f in parquet_files])
        else:
            raise ValueError(f"Path must be a file or directory: {base_path}")

        # Test the dataset
        schema = dataset.schema
        logger.info(
            "PyArrow direct connection established",
            columns=len(schema),
            base_path=base_path,
        )

        return {"dataset": dataset, "base_path": base_path, "type": "pyarrow"}

    def _connect_pandas_direct(self) -> Any:
        """
        Connect using Pandas for Parquet access with fallback engines.

        Returns:
            Connection info with file listings

        Raises:
            ImportError: If pandas is not installed
            Exception: If connection fails
        """
        import pandas as pd

        base_path = self.connection_config.host

        # Validate base path exists
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Base directory not found: {base_path}")

        logger.info("Attempting pandas direct connection to Parquet files")

        path = Path(base_path)

        # Find all parquet files
        if path.is_file():
            if path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
                raise ValueError(
                    f"File must have one of these extensions: {self.SUPPORTED_EXTENSIONS}"
                )
            parquet_files = [path]
        elif path.is_dir():
            parquet_files = list(path.glob("**/*.parquet")) + list(path.glob("**/*.pq"))
            if not parquet_files:
                raise FileNotFoundError(
                    f"No Parquet files found in directory: {base_path}"
                )
        else:
            raise ValueError(f"Path must be a file or directory: {base_path}")

        # Try different engines in order of preference
        engines = ["pyarrow", "fastparquet", "auto"]
        working_engine = None

        for engine in engines:
            try:
                # Test with first file
                test_df = pd.read_parquet(parquet_files[0], engine=engine, nrows=1)
                working_engine = engine
                break
            except Exception as e:
                logger.debug(f"Engine {engine} failed: {e}")
                continue

        if not working_engine:
            raise RuntimeError("No compatible Parquet engine found")

        connection_info = {
            "files": [str(f) for f in parquet_files],
            "base_path": base_path,
            "engine": working_engine,
            "type": "pandas",
        }

        logger.info(
            "Pandas direct connection established",
            files=len(parquet_files),
            engine=working_engine,
            base_path=base_path,
        )

        return connection_info

    def _execute_query_internal(
        self, query: str, params: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Internal method to execute query (limited SQL support for files).

        Args:
            query: Limited SQL-like query
            params: Optional query parameters

        Returns:
            DataFrame containing query results
        """
        if not self.connection:
            raise RuntimeError("No active connection. Call connect() first.")

        # Parse simple queries like "SELECT * FROM table_name LIMIT 100"
        query_lower = query.lower().strip()

        if not query_lower.startswith("select"):
            raise ValueError(
                "Only SELECT queries are supported for file-based connectors"
            )

        # Extract table name and limit
        table_name = self._extract_table_name(query)
        limit = self._extract_limit(query)

        # Load the data
        if self.connection_method_used == ConnectionMethod.PYARROW_DIRECT:
            dataset = self.connection["dataset"]

            if limit:
                # Use PyArrow to read limited rows
                table = dataset.head(limit)
                df = table.to_pandas()
            else:
                # Read full dataset
                df = dataset.to_table().to_pandas()

        else:  # PANDAS_DIRECT
            files = self.connection["files"]
            engine = self.connection["engine"]

            # Find the specific file if table name is provided
            if table_name and table_name != "*":
                matching_files = [f for f in files if table_name in os.path.basename(f)]
                if matching_files:
                    files = matching_files[:1]  # Use first match

            # Read data from files
            dfs = []
            for file_path in files:
                try:
                    if limit and not dfs:  # Only apply limit to first file
                        df_part = pd.read_parquet(file_path, engine=engine, nrows=limit)
                    else:
                        df_part = pd.read_parquet(file_path, engine=engine)
                    dfs.append(df_part)

                    # If we have a limit and enough rows, stop
                    if limit and sum(len(df) for df in dfs) >= limit:
                        break
                except Exception as e:
                    logger.warning(f"Failed to read file {file_path}: {e}")
                    continue

            if not dfs:
                return pd.DataFrame()

            # Combine dataframes
            df = pd.concat(dfs, ignore_index=True)

            if limit and len(df) > limit:
                df = df.head(limit)

        return df

    def _extract_table_name(self, query: str) -> Optional[str]:
        """Extract table name from simple SQL query."""
        # Very basic SQL parsing - in practice, you'd use a proper SQL parser
        import re

        # Match patterns like "FROM table_name" or "FROM 'table_name'"
        match = re.search(r'from\s+([\'"`]?)(\w+)\1', query, re.IGNORECASE)
        if match:
            return match.group(2)

        return None

    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value from SQL query."""
        import re

        match = re.search(r"limit\s+(\d+)", query, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return None

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        Get metadata for a Parquet file/table.

        Args:
            table_name: Name of the file/table (can be filename or pattern)
            schema_name: Not used for file connectors

        Returns:
            TableMetadata object
        """
        if not self.connection:
            self.connect()

        # Find the specific file
        if self.connection_method_used == ConnectionMethod.PYARROW_DIRECT:
            dataset = self.connection["dataset"]
            schema = dataset.schema

            # Convert PyArrow schema to column metadata
            columns = []
            for field in schema:
                # Map PyArrow types to common types
                common_type = self._map_pyarrow_type(field.type)

                column = ColumnMetadata(
                    name=field.name,
                    data_type=common_type,
                    nullable=field.nullable,
                    description=str(field.type),
                )
                columns.append(column)

            # Try to get row count (expensive operation)
            try:
                row_count = dataset.count_rows()
            except Exception:
                row_count = None

        else:  # PANDAS_DIRECT
            files = self.connection["files"]
            engine = self.connection["engine"]

            # Find matching file
            matching_files = [f for f in files if table_name in os.path.basename(f)]
            if not matching_files:
                matching_files = files[:1]  # Use first file as default

            # Read schema from first matching file
            sample_df = pd.read_parquet(
                matching_files[0], engine=engine, nrows=0
            )  # Schema only

            columns = []
            for col_name, dtype in sample_df.dtypes.items():
                common_type = self._map_pandas_type(dtype)

                column = ColumnMetadata(
                    name=col_name,
                    data_type=common_type,
                    nullable=True,  # Parquet doesn't enforce non-null
                    description=str(dtype),
                )
                columns.append(column)

            # Get row count from all matching files
            row_count = 0
            for file_path in matching_files:
                try:
                    # Use PyArrow for efficient row counting if available
                    try:
                        import pyarrow.parquet as pq

                        parquet_file = pq.ParquetFile(file_path)
                        row_count += parquet_file.metadata.num_rows
                    except ImportError:
                        # Fallback to pandas
                        df = pd.read_parquet(
                            file_path, engine=engine, columns=[columns[0].name]
                        )
                        row_count += len(df)
                except Exception:
                    row_count = None
                    break

        # Get file size
        size_bytes = 0
        if self.connection_method_used == ConnectionMethod.PYARROW_DIRECT:
            files = self.connection["dataset"].files
        else:
            files = matching_files

        for file_path in files:
            try:
                size_bytes += os.path.getsize(file_path)
            except Exception:
                pass

        return TableMetadata(
            columns=columns,
            row_count=row_count,
            size_bytes=size_bytes,
            statistics={
                "file_format": "parquet",
                "base_path": self.connection["base_path"],
                "files": len(files) if isinstance(files, list) else 1,
            },
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get sample data from Parquet file.

        Args:
            table_name: Name of the file/table
            limit: Maximum number of rows to return
            schema_name: Not used for file connectors

        Returns:
            DataFrame containing sample data
        """
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self._execute_query_internal(query)

    def _list_tables_internal(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all Parquet files as "tables".

        Args:
            schema_name: Not used for file connectors

        Returns:
            List of file names (without extensions)
        """
        if not self.connection:
            self.connect()

        if self.connection_method_used == ConnectionMethod.PYARROW_DIRECT:
            files = self.connection["dataset"].files
        else:
            files = self.connection["files"]

        # Return filenames without extensions as table names
        table_names = []
        for file_path in files:
            basename = os.path.basename(file_path)
            name_without_ext = os.path.splitext(basename)[0]
            table_names.append(name_without_ext)

        return sorted(set(table_names))  # Remove duplicates and sort

    def _list_schemas_internal(self) -> List[str]:
        """
        List schemas (not applicable for files, return empty list).

        Returns:
            Empty list (files don't have schemas)
        """
        return []

    def _get_test_query(self) -> str:
        """
        Return a simple query to test the connection.

        Returns:
            A simple query string
        """
        return "SELECT 1 as test_column"

    def _map_pyarrow_type(self, arrow_type) -> str:
        """Map PyArrow types to common types."""
        import pyarrow as pa

        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "string"
        elif pa.types.is_integer(arrow_type):
            if arrow_type == pa.int8():
                return "int8"
            elif arrow_type == pa.int16():
                return "int16"
            elif arrow_type == pa.int32():
                return "int32"
            else:
                return "int64"
        elif pa.types.is_floating(arrow_type):
            if arrow_type == pa.float32():
                return "float32"
            else:
                return "float64"
        elif pa.types.is_boolean(arrow_type):
            return "boolean"
        elif pa.types.is_timestamp(arrow_type):
            return "timestamp"
        elif pa.types.is_date(arrow_type):
            return "date"
        elif pa.types.is_time(arrow_type):
            return "time"
        elif pa.types.is_decimal(arrow_type):
            return "decimal"
        else:
            return "string"  # Default fallback

    def _map_pandas_type(self, pandas_type) -> str:
        """Map pandas dtypes to common types."""
        type_str = str(pandas_type)

        if pandas_type.name.startswith("int"):
            if "int8" in type_str:
                return "int8"
            elif "int16" in type_str:
                return "int16"
            elif "int32" in type_str:
                return "int32"
            else:
                return "int64"
        elif pandas_type.name.startswith("float"):
            if "float32" in type_str:
                return "float32"
            else:
                return "float64"
        elif pandas_type.name == "bool":
            return "boolean"
        elif pandas_type.name == "object":
            return "string"
        elif "datetime" in type_str:
            return "timestamp"
        elif "date" in type_str:
            return "date"
        else:
            return "string"  # Default fallback
