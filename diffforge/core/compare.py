from typing import Hashable, MutableMapping, Type, Optional, Union, Dict, Any
import structlog
logger = structlog.get_logger(__name__)

class Diffcheck(): 
    _conn1 : Any
    _conn2 : Any
    _db1: Any
    _db2: Any

    def __init__(self, connection1, connection2, db1, db2):
        super().__init__()
        self._conn1 = connection1
        self._conn2 = connection2
        self._db1 = db1
        self._db2 = db2

    def get_column_list(self, conn1, table_name):

        QUERY =  f"SELECT * FROM {table_name}  LIMIT 0"
        cur = conn1.connection.cursor()
        cur.execute(QUERY)
        columns = [desc[0] for desc in cur.description]
        return columns
    
    def check_cols(self, column_list_a, column_list_b):
        return column_list_a == column_list_b

    def data_diff(self, algorithm):

        keys_a = self._db1['keys']
        keys_b = self._db2['keys']
        table_a = self._db1['fq_table_name']
        table_b = self._db2['fq_table_name']

        # TODO Split Logic
        # TODO CHECK SCHEMA

        # Columns Check
        column_list_a = self.get_column_list(self._conn1, self._db1['fq_table_name'])
        column_list_b = self.get_column_list(self._conn2, self._db2['fq_table_name'])

        if not(self.check_cols(column_list_a, column_list_b)):
            raise ValueError("Column list mismatches")

        if(algorithm == "JOIN_DIFF"):
            join_q = self.constuct_join(keys_a, keys_b, table_a, table_b, column_list_a, column_list_b, "LEFT")
            cur = self._conn1.connection.cursor()
            cur.execute(join_q)
            row_count = cur.rowcount
            if(row_count==0):
                logger.info(f"Segment equal - ROW_COUNT: {row_count}")
            else:
                logger.info(f"Segment not equal - ROW_COUNT:{row_count}")
            

    def check_schema(table_a, table_b):
        logger.info("Inside check schema")
    
    def compare():
        logger.info("Inside Compare")


    def constuct_join(self, keys_a, keys_b, table_a, table_b, column_list_a, column_list_b, join_type="INNER"):
        
        """
        Build a SQL JOIN statement.
        join_type: INNER, LEFT, RIGHT, FULL
        """

        concat_dialect_a = self._conn1.DIALECTS["concatination"]
        concat_dialect_b = self._conn2.DIALECTS["concatination"]
        alias_a = "tbl1"
        alias_b = "tbl2"

        if len(keys_a) != len(keys_b):
            raise ValueError("Join keys must have the same length")
        
        # Build ON clause
        on_conditions = [f"tbl1.{ka} = tbl2.{kb}" for ka, kb in zip(keys_a, keys_b)]
        on_clause = " AND ".join(on_conditions)

        hash_query_a = self._conn1.DIALECTS["hash"].replace("{COL}", f"{concat_dialect_a}".join([f"{alias_a}." + col for col in column_list_a]))
        hash_query_b = self._conn2.DIALECTS["hash"].replace("{COL}", f"{concat_dialect_b}".join([f"{alias_b}." + col for col in column_list_b]))
        
        sql = f"""
        SELECT 1
        FROM {table_a} {alias_a}
        {join_type} JOIN {table_b} {alias_b} ON {on_clause}
        where {hash_query_a}!={hash_query_b}
        """

        logger.info(f"Query constructed: {sql.strip()}")
        return sql.strip()

