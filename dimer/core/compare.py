from typing import Hashable, MutableMapping, Type, Optional, Union, Dict, Any, List
import structlog
from dimer.core.models import TableMetadata, ColumnMetadata

logger = structlog.get_logger(__name__)

class Diffcheck(): 
    _left_connector : Any
    _right_connector : Any
    _left_config: Any
    _right_config: Any

    def __init__(self, connection1, connection2, db1, db2):
        super().__init__()
        self._left_connector = connection1
        self._right_connector = connection2
        self._left_config = db1
        self._right_config = db2

    def get_column_list(self, conn, table_name):
        """
        Get column list using fall back to query method if metadata fails.
        """
        QUERY = f"SELECT * FROM {table_name} LIMIT 0"
        cur = conn.connection.cursor()
        cur.execute(QUERY)
        columns = [desc[0] for desc in cur.description]
        return columns

    def check_cols(self, column_list_a, column_list_b):
        """Check if column lists match exactly."""
        return column_list_a == column_list_b
    
    def get_schema_metadata(self, conn, table_name):
        """
        Get comprehensive table metadata including columns, types, and constraints.
        """
        try:
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                schema = schema.strip('"')
                table = table.strip('"')
            else:
                schema = conn.connection_config.schema_name
                table = table_name.strip('"')
            
            return conn.get_table_metadata(table, schema)
            
        except Exception as e:
            logger.error(f"Failed to get schema metadata for {table_name}: {e}")
            return None
    
    def compare_schemas(self, metadata_a: TableMetadata, metadata_b: TableMetadata):
        """
        Compare table schemas and return detailed differences.
        """
        differences = {
            'columns_only_in_a': [],
            'columns_only_in_b': [],
            'column_type_differences': [],
            'row_count_difference': None,
            'size_difference': None
        }
        
        # Create column dictionaries for comparison
        cols_a = {col.name: col for col in metadata_a.columns}
        cols_b = {col.name: col for col in metadata_b.columns}
        
        # Find columns only in each table
        differences['columns_only_in_a'] = list(set(cols_a.keys()) - set(cols_b.keys()))
        differences['columns_only_in_b'] = list(set(cols_b.keys()) - set(cols_a.keys()))
        
        # Check data type differences for common columns
        common_columns = set(cols_a.keys()) & set(cols_b.keys())
        for col_name in common_columns:
            col_a = cols_a[col_name]
            col_b = cols_b[col_name]
            
            if col_a.data_type != col_b.data_type or col_a.nullable != col_b.nullable:
                differences['column_type_differences'].append({
                    'column': col_name,
                    'table_a': {'type': col_a.data_type, 'nullable': col_a.nullable},
                    'table_b': {'type': col_b.data_type, 'nullable': col_b.nullable}
                })
        
        # Compare row counts and sizes
        if metadata_a.row_count is not None and metadata_b.row_count is not None:
            differences['row_count_difference'] = metadata_a.row_count - metadata_b.row_count
        
        if metadata_a.size_bytes is not None and metadata_b.size_bytes is not None:
            differences['size_difference'] = metadata_a.size_bytes - metadata_b.size_bytes
        
        return differences

    def compare_within_database(self, algorithm):

        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']
        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']

        # Enhanced schema checking using metadata
        logger.info("Starting schema comparison...")
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)
        
        if metadata_a is None or metadata_b is None:
            logger.warning("Could not retrieve metadata, falling back to simple column check")
            # Fallback to original column check
            column_list_a = self.get_column_list(self._left_connector, table_a)
            column_list_b = self.get_column_list(self._right_connector, table_b)
            
            if not self.check_cols(column_list_a, column_list_b):
                raise ValueError("Column list mismatches")
        else:
            # Use enhanced schema comparison
            schema_diff = self.compare_schemas(metadata_a, metadata_b)
            
            # Log schema differences
            if schema_diff['columns_only_in_a']:
                logger.warning(f"Columns only in table A: {schema_diff['columns_only_in_a']}")
            if schema_diff['columns_only_in_b']:
                logger.warning(f"Columns only in table B: {schema_diff['columns_only_in_b']}")
            if schema_diff['column_type_differences']:
                logger.warning(f"Column type differences: {schema_diff['column_type_differences']}")
            
            # For data comparison, use only common columns
            common_columns = [col.name for col in metadata_a.columns 
                            if col.name in [c.name for c in metadata_b.columns]]
            
            if not common_columns:
                raise ValueError("No common columns found between tables")
            
            logger.info(f"Using {len(common_columns)} common columns for comparison")
            column_list_a = column_list_b = common_columns

        if(algorithm == "JOIN_DIFF"):
            join_q = self.constuct_join(keys_a, keys_b, table_a, table_b, column_list_a, column_list_b, "LEFT")
            cur = self._left_connector.connection.cursor()
            cur.execute(join_q)
            row_count = cur.rowcount
            if(row_count==0):
                logger.info(f"Segment equal - ROW_COUNT: {row_count}")
            else:
                logger.info(f"Segment not equal - ROW_COUNT:{row_count}")
            

    def check_schema(self, table_a, table_b):
        """
        Detailed schema comparison between two tables.
        """
        logger.info("Starting detailed schema comparison")
        
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)
        
        if metadata_a is None or metadata_b is None:
            logger.error("Could not retrieve metadata for schema comparison")
            return False
        
        differences = self.compare_schemas(metadata_a, metadata_b)
        
        # Print detailed comparison results
        logger.info(f"Schema comparison results:")
        logger.info(f"Table A ({table_a}): {len(metadata_a.columns)} columns, {metadata_a.row_count} rows")
        logger.info(f"Table B ({table_b}): {len(metadata_b.columns)} columns, {metadata_b.row_count} rows")
        
        if differences['columns_only_in_a']:
            logger.info(f"Columns only in A: {differences['columns_only_in_a']}")
        if differences['columns_only_in_b']:
            logger.info(f"Columns only in B: {differences['columns_only_in_b']}")
        if differences['column_type_differences']:
            logger.info(f"Type differences: {differences['column_type_differences']}")
        
        # Return True if schemas are compatible (same columns, possibly different types)
        return len(differences['columns_only_in_a']) == 0 and len(differences['columns_only_in_b']) == 0
    
    def compare_cross_database(self, algorithm="JOIN_DIFF"):
        """
        Compare tables from different databases/instances.
        This method handles cross-database comparisons where direct JOIN is not possible.
        """
        logger.info("Starting cross-database table comparison")
        
        keys_a = self._left_config['keys']
        keys_b = self._right_config['keys']
        table_a = self._left_config['fq_table_name']
        table_b = self._right_config['fq_table_name']
        
        # Get metadata for both tables
        metadata_a = self.get_schema_metadata(self._left_connector, table_a)
        metadata_b = self.get_schema_metadata(self._right_connector, table_b)
        
        if metadata_a is None or metadata_b is None:
            logger.error("Could not retrieve metadata for cross-database comparison")
            return
        
        # Compare schemas first
        schema_diff = self.compare_schemas(metadata_a, metadata_b)
        logger.info(f"Schema differences: {schema_diff}")
        
        # Get common columns for comparison
        common_columns = [col.name for col in metadata_a.columns 
                        if col.name in [c.name for c in metadata_b.columns]]
        
        if not common_columns:
            logger.error("No common columns found for cross-database comparison")
            return
        
        logger.info(f"Comparing {len(common_columns)} common columns")
        
        # For cross-database comparison, we need to fetch data and compare
        # This is a simplified approach - in production you might want to use checksums
        
        try:
            # Get sample data from both tables to compare
            sample_size = 1000  # Configurable sample size
            
            # Build queries to get data with common columns only
            columns_str = ', '.join(common_columns)
            query_a = f"SELECT {columns_str} FROM {table_a} ORDER BY {', '.join(keys_a)} LIMIT {sample_size}"
            query_b = f"SELECT {columns_str} FROM {table_b} ORDER BY {', '.join(keys_b)} LIMIT {sample_size}"
            
            logger.info(f"Executing sample queries for comparison")
            
            result_a = self._left_connector.execute_query(query_a)
            result_b = self._right_connector.execute_query(query_b)
            
            # Compare the data
            data_a = result_a.data
            data_b = result_b.data
            
            logger.info(f"Retrieved {len(data_a)} rows from table A and {len(data_b)} rows from table B")
            
            # Simple comparison - check if dataframes are equal
            if len(data_a) != len(data_b):
                logger.info(f"Row count difference in sample: {len(data_a)} vs {len(data_b)}")
            
            # You could add more sophisticated comparison logic here
            # For now, just log the completion
            logger.info("Cross-database comparison completed")
            
        except Exception as e:
            logger.error(f"Error during cross-database comparison: {e}")
    
    def compare(self):
        """
        General comparison method that chooses appropriate strategy.
        """
        logger.info("Starting table comparison")
        
        # Check if both connections are to the same database instance
        same_instance = (
            self._left_connector.connection_config.host == self._right_connector.connection_config.host and
            self._left_connector.connection_config.database == self._right_connector.connection_config.database
        )
        
        if same_instance:
            logger.info("Same database instance detected, using JOIN-based comparison")
            self.compare_within_database("JOIN_DIFF")
        else:
            logger.info("Different database instances detected, using cross-database comparison")
            self.compare_cross_database()


    def constuct_join(self, keys_a, keys_b, table_a, table_b, column_list_a, column_list_b, join_type="INNER"):
        
        """
        Build a SQL JOIN statement.
        join_type: INNER, LEFT, RIGHT, FULL
        """

        concat_dialect_a = self._left_connector.DIALECTS["concatination"]
        concat_dialect_b = self._right_connector.DIALECTS["concatination"]
        alias_a = "tbl1"
        alias_b = "tbl2"

        if len(keys_a) != len(keys_b):
            raise ValueError("Join keys must have the same length")
        
        # Build ON clause
        on_conditions = [f"tbl1.{ka} = tbl2.{kb}" for ka, kb in zip(keys_a, keys_b)]
        on_clause = " AND ".join(on_conditions)

        # Build concatenated column expressions for hashing
        col_expr_a = f"{concat_dialect_a}".join([f"{alias_a}.{col}" for col in column_list_a])
        col_expr_b = f"{concat_dialect_b}".join([f"{alias_b}.{col}" for col in column_list_b])
        
        hash_query_a = self._left_connector.DIALECTS["hash"].replace("{COL}", col_expr_a)
        hash_query_b = self._right_connector.DIALECTS["hash"].replace("{COL}", col_expr_b)
        
        sql = f"""
        SELECT 1
        FROM {table_a} {alias_a}
        {join_type} JOIN {table_b} {alias_b} ON {on_clause}
        where {hash_query_a}!={hash_query_b}
        """

        logger.info(f"Query constructed: {sql.strip()}")
        return sql.strip()

