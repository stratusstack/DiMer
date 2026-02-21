"""CSV file connector with multiple access strategies."""

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

logger = structlog.get_logger(__name__)


class CSVConnector(DataSourceConnector):
    """CSV file connector with multiple access strategies."""

    # CSV-specific configuration
    SUPPORTED_EXTENSIONS = [".csv", ".tsv", ".txt"]
    DEFAULT_ENCODING = "utf-8"
    CHUNK_SIZE = 10000

    def get_required_params(self) -> List[str]:
        """Return list of required connection parameters for CSV files."""
        # For file-based connectors, we use 'host' as the base directory path
        return ["host"]

    def get_connection_methods(self) -> List[ConnectionMethod]:
        """
        Return methods in order of preference (fastest/most efficient first).

        Returns:
            Ordered list of connection methods to try
        """
        return [
            ConnectionMethod.PANDAS_DIRECT,  # Pandas with optimizations
        ]

    def _connect_pandas_direct(self) -> Any:
        """
        Connect using Pandas for CSV access with various optimizations.

        Returns:
            Connection info with file listings and CSV parameters

        Raises:
            ImportError: If pandas is not installed
            Exception: If connection fails
        """
        import pandas as pd

        base_path = self.connection_config.host

        # Validate base path exists
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Base directory not found: {base_path}")

        logger.info("Attempting pandas direct connection to CSV files")

        path = Path(base_path)

        # Find all CSV files
        if path.is_file():
            if path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
                raise ValueError(
                    f"File must have one of these extensions: {self.SUPPORTED_EXTENSIONS}"
                )
            csv_files = [path]
        elif path.is_dir():
            csv_files = []
            for ext in self.SUPPORTED_EXTENSIONS:
                csv_files.extend(path.glob(f"**/*{ext}"))
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found in directory: {base_path}")
        else:
            raise ValueError(f"Path must be a file or directory: {base_path}")

        # Auto-detect CSV parameters from the first file
        first_file = csv_files[0]
        csv_params = self._detect_csv_params(str(first_file))

        connection_info = {
            "files": [str(f) for f in csv_files],
            "base_path": base_path,
            "csv_params": csv_params,
            "type": "pandas",
        }

        logger.info(
            "Pandas CSV connection established",
            files=len(csv_files),
            separator=csv_params["sep"],
            encoding=csv_params["encoding"],
            base_path=base_path,
        )

        return connection_info

    def _detect_csv_params(self, file_path: str) -> Dict[str, Any]:
        """
        Auto-detect CSV parameters like separator, encoding, etc.

        Args:
            file_path: Path to CSV file to analyze

        Returns:
            Dictionary of CSV parameters
        """
        import csv

        # Default parameters
        params = {
            "sep": ",",
            "encoding": self.DEFAULT_ENCODING,
            "header": 0,
            "quotechar": '"',
            "escapechar": None,
            "na_values": ["", "NA", "NULL", "null", "None"],
        }

        # Override with user-specified parameters
        extra_params = self.connection_config.extra_params
        if extra_params:
            params.update(
                {
                    "sep": extra_params.get("separator", params["sep"]),
                    "encoding": extra_params.get("encoding", params["encoding"]),
                    "header": extra_params.get("header", params["header"]),
                    "quotechar": extra_params.get("quotechar", params["quotechar"]),
                    "na_values": extra_params.get("na_values", params["na_values"]),
                }
            )

        # If separator not specified, try to detect it
        if extra_params.get("separator") is None:
            try:
                with open(file_path, "r", encoding=params["encoding"], newline="") as f:
                    # Read first few lines to detect separator
                    sample = f.read(8192)

                    # Use csv.Sniffer to detect delimiter
                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(sample, delimiters=",;\t|").delimiter
                    params["sep"] = delimiter

            except Exception as e:
                logger.debug(f"Failed to auto-detect separator: {e}, using comma")

        # Handle TSV files
        if file_path.lower().endswith(".tsv"):
            params["sep"] = "\t"

        return params

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
        columns = self._extract_columns(query)

        files = self.connection["files"]
        csv_params = self.connection["csv_params"]

        # Find the specific file if table name is provided
        if table_name and table_name != "*":
            matching_files = [f for f in files if table_name in os.path.basename(f)]
            if matching_files:
                files = matching_files[:1]  # Use first match

        # Read data from files
        dfs = []
        for file_path in files:
            try:
                read_params = csv_params.copy()

                # Handle column selection
                if columns and columns != ["*"]:
                    read_params["usecols"] = columns

                # Handle row limit
                if limit and not dfs:  # Only apply limit to first file
                    read_params["nrows"] = limit

                df_part = pd.read_csv(file_path, **read_params)
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

    def _extract_columns(self, query: str) -> List[str]:
        """Extract column names from SELECT query."""
        import re

        # Very basic column extraction
        select_match = re.search(
            r"select\s+(.*?)\s+from", query, re.IGNORECASE | re.DOTALL
        )
        if select_match:
            columns_str = select_match.group(1).strip()
            if columns_str == "*":
                return ["*"]
            else:
                # Split by comma and clean up
                columns = [col.strip().strip("\"`'") for col in columns_str.split(",")]
                return columns

        return ["*"]

    def get_table_metadata(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> TableMetadata:
        """
        Get metadata for a CSV file/table.

        Args:
            table_name: Name of the file/table (can be filename or pattern)
            schema_name: Not used for file connectors

        Returns:
            TableMetadata object
        """
        if not self.connection:
            self.connect()

        files = self.connection["files"]
        csv_params = self.connection["csv_params"]

        # Find matching file
        matching_files = [f for f in files if table_name in os.path.basename(f)]
        if not matching_files:
            matching_files = files[:1]  # Use first file as default

        # Read schema from first matching file (just headers and a few rows)
        sample_params = csv_params.copy()
        sample_params["nrows"] = 100  # Sample for type inference

        sample_df = pd.read_csv(matching_files[0], **sample_params)

        columns = []
        for col_name, dtype in sample_df.dtypes.items():
            common_type = self._map_pandas_type(dtype)

            # Check for nullability by sampling non-null ratio
            non_null_ratio = sample_df[col_name].notna().mean()
            nullable = non_null_ratio < 1.0

            column = ColumnMetadata(
                name=col_name,
                data_type=common_type,
                nullable=nullable,
                description=f"CSV column, pandas dtype: {dtype}",
            )
            columns.append(column)

        # Get row count from all matching files (efficient counting)
        row_count = 0
        for file_path in matching_files:
            try:
                # Use a more efficient method to count lines
                with open(file_path, "r", encoding=csv_params["encoding"]) as f:
                    # Skip header if present
                    if csv_params.get("header") == 0:
                        next(f)
                    row_count += sum(1 for _ in f)
            except Exception:
                # Fallback to pandas
                try:
                    df = pd.read_csv(file_path, usecols=[0], **csv_params)
                    row_count += len(df)
                except Exception:
                    row_count = None
                    break

        # Get file size
        size_bytes = 0
        for file_path in matching_files:
            try:
                size_bytes += os.path.getsize(file_path)
            except Exception:
                pass

        return TableMetadata(
            columns=columns,
            row_count=row_count,
            size_bytes=size_bytes,
            statistics={
                "file_format": "csv",
                "separator": csv_params["sep"],
                "encoding": csv_params["encoding"],
                "base_path": self.connection["base_path"],
                "files": len(matching_files),
            },
        )

    def get_sample_data(
        self, table_name: str, limit: int = 10, schema_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get sample data from CSV file.

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
        List all CSV files as "tables".

        Args:
            schema_name: Not used for file connectors

        Returns:
            List of file names (without extensions)
        """
        if not self.connection:
            self.connect()

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
        return "SELECT * FROM sample LIMIT 1"

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

    def close(self) -> None:
        """Clean up CSV connection resources (nothing to clean up for files)."""
        try:
            if self.connection:
                self.connection = None
                self.connection_method_used = None

            logger.info("CSV connection closed successfully")

        except Exception as e:
            logger.error("Error closing CSV connection", error=str(e))
