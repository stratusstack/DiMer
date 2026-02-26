"""Unified type system for cross-database type mapping."""

from typing import Dict, Set


class DataTypeMapper:
    """Unified type system across all data sources."""

    # Common type system that all sources map to
    COMMON_TYPES = {
        "string",
        "text",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "float64",
        "decimal",
        "boolean",
        "date",
        "datetime",
        "timestamp",
        "time",
        "binary",
        "json",
        "array",
        "object",
        "uuid",
    }

    # Type mappings for different data sources
    TYPE_MAPPINGS = {
        "snowflake": {
            "VARCHAR": "string",
            "CHAR": "string",
            "TEXT": "text",
            "STRING": "string",
            "NUMBER": "decimal",
            "DECIMAL": "decimal",
            "NUMERIC": "decimal",
            "INT": "int64",
            "INTEGER": "int64",
            "BIGINT": "int64",
            "SMALLINT": "int16",
            "TINYINT": "int8",
            "FLOAT": "float64",
            "FLOAT4": "float32",
            "FLOAT8": "float64",
            "DOUBLE": "float64",
            "REAL": "float32",
            "BOOLEAN": "boolean",
            "DATE": "date",
            "DATETIME": "datetime",
            "TIME": "time",
            "TIMESTAMP": "timestamp",
            "TIMESTAMP_LTZ": "timestamp",
            "TIMESTAMP_NTZ": "timestamp",
            "TIMESTAMP_TZ": "timestamp",
            "VARIANT": "json",
            "OBJECT": "object",
            "ARRAY": "array",
            "GEOGRAPHY": "string",
            "GEOMETRY": "string",
            "BINARY": "binary",
            "VARBINARY": "binary",
        },
        "postgresql": {
            "varchar": "string",
            "char": "string",
            "text": "text",
            "character": "string",
            "character varying": "string",
            "int2": "int16",
            "int4": "int32",
            "int8": "int64",
            "smallint": "int16",
            "integer": "int32",
            "bigint": "int64",
            "decimal": "decimal",
            "numeric": "decimal",
            "real": "float32",
            "float4": "float32",
            "double precision": "float64",
            "float8": "float64",
            "boolean": "boolean",
            "bool": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "timestamptz": "timestamp",
            "timestamp with time zone": "timestamp",
            "timestamp without time zone": "timestamp",
            "time": "time",
            "timetz": "time",
            "json": "json",
            "jsonb": "json",
            "uuid": "uuid",
            "bytea": "binary",
            "array": "array",
            "point": "string",
            "line": "string",
            "lseg": "string",
            "box": "string",
            "path": "string",
            "polygon": "string",
            "circle": "string",
        },
        "mysql": {
            "VARCHAR": "string",
            "CHAR": "string",
            "TEXT": "text",
            "TINYTEXT": "string",
            "MEDIUMTEXT": "text",
            "LONGTEXT": "text",
            "TINYINT": "int8",
            "SMALLINT": "int16",
            "MEDIUMINT": "int32",
            "INT": "int32",
            "INTEGER": "int32",
            "BIGINT": "int64",
            "DECIMAL": "decimal",
            "NUMERIC": "decimal",
            "FLOAT": "float32",
            "DOUBLE": "float64",
            "REAL": "float64",
            "BIT": "boolean",
            "BOOLEAN": "boolean",
            "DATE": "date",
            "DATETIME": "datetime",
            "TIMESTAMP": "timestamp",
            "TIME": "time",
            "YEAR": "int16",
            "JSON": "json",
            "BINARY": "binary",
            "VARBINARY": "binary",
            "BLOB": "binary",
            "TINYBLOB": "binary",
            "MEDIUMBLOB": "binary",
            "LONGBLOB": "binary",
            "ENUM": "string",
            "SET": "string",
        },
        "bigquery": {
            "STRING": "string",
            "BYTES": "binary",
            "INTEGER": "int64",
            "INT64": "int64",
            "FLOAT": "float64",
            "FLOAT64": "float64",
            "NUMERIC": "decimal",
            "BIGNUMERIC": "decimal",
            "BOOLEAN": "boolean",
            "BOOL": "boolean",
            "TIMESTAMP": "timestamp",
            "DATE": "date",
            "TIME": "time",
            "DATETIME": "datetime",
            "JSON": "json",
            "ARRAY": "array",
            "STRUCT": "object",
            "GEOGRAPHY": "string",
        },
        "databricks": {
            "STRING": "string",
            "BINARY": "binary",
            "BOOLEAN": "boolean",
            "BYTE": "int8",
            "SHORT": "int16",
            "INT": "int32",
            "LONG": "int64",
            "FLOAT": "float32",
            "DOUBLE": "float64",
            "DECIMAL": "decimal",
            "DATE": "date",
            "TIMESTAMP": "timestamp",
            "ARRAY": "array",
            "MAP": "object",
            "STRUCT": "object",
            "VOID": "string",
        },
    }

    @classmethod
    def map_type(cls, source_type: str, native_type: str) -> str:
        """
        Map a source-specific type to the common type system.

        Args:
            source_type: The data source (e.g., 'snowflake', 'postgresql')
            native_type: The native type name from the source

        Returns:
            The corresponding common type name

        Raises:
            ValueError: If source_type is not supported
            KeyError: If native_type is not found for the source
        """
        source_type = source_type.lower()
        native_type = native_type.upper()

        if source_type not in cls.TYPE_MAPPINGS:
            raise ValueError(f"Unsupported source type: {source_type}")

        type_mapping = cls.TYPE_MAPPINGS[source_type]

        # Try exact match first
        if native_type in type_mapping:
            return type_mapping[native_type]

        # Try case-insensitive match
        for native_key, common_type in type_mapping.items():
            if native_key.upper() == native_type:
                return common_type

        # Try partial matching for parameterized types (e.g., VARCHAR(255))
        base_type = native_type.split("(")[0].strip()
        if base_type in type_mapping:
            return type_mapping[base_type]
        for native_key, common_type in type_mapping.items():
            if native_key.upper() == base_type:
                return common_type

        # Default fallback
        return "string"

    @classmethod
    def get_supported_sources(cls) -> Set[str]:
        """Return set of supported data source types."""
        return set(cls.TYPE_MAPPINGS.keys())

    @classmethod
    def get_common_types(cls) -> Set[str]:
        """Return set of common type names."""
        return cls.COMMON_TYPES.copy()

    @classmethod
    def get_source_types(cls, source_type: str) -> Set[str]:
        """
        Get all native types supported by a specific source.

        Args:
            source_type: The data source name

        Returns:
            Set of native type names for the source
        """
        source_type = source_type.lower()
        if source_type not in cls.TYPE_MAPPINGS:
            return set()
        return set(cls.TYPE_MAPPINGS[source_type].keys())
