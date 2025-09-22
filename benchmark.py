import time
import pandas as pd
import tracemalloc
import snowflake.connector
from snowflake.snowpark import Session

# -----------------------
# Connection Parameters
# -----------------------
CONN_PARAMS = {
    "user": "",
    "password": "",
    "account": "",  # e.g. "xy12345.us-east-1"
    "warehouse": "COMPUTE_WH"
    # "database": "YOUR_DB",
    # "schema": "PUBLIC"
}

TEST_QUERY = "SELECT * FROM  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER LIMIT 10"

def run_with_memory(func, *args, **kwargs):
    tracemalloc.start()
    start = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak / (1024 * 1024), result  # return peak MB

# -----------------------
# 1. Default Connector (read_sql)
# -----------------------
def default_fetch():
    conn = snowflake.connector.connect(**CONN_PARAMS)
    df = pd.read_sql(TEST_QUERY, conn)
    conn.close()
    return df.shape

# -----------------------
# 2. Arrow Connector (fetch_pandas_all)
# -----------------------
def pandas_fetch():
    conn = snowflake.connector.connect(**CONN_PARAMS)
    cur = conn.cursor()
    cur.execute(TEST_QUERY)
    df = cur.fetch_pandas_all()
    conn.close()
    return df.shape

# -----------------------
# 3. Arrow Connector (arrow_fetch)
# -----------------------
def arrow_fetch():
    conn = snowflake.connector.connect(**CONN_PARAMS)
    cur = conn.cursor()
    cur.execute(TEST_QUERY)
    arr = cur.fetch_arrow_all()
    conn.close()
    return (arr.num_rows, len(arr.column_names))


# -----------------------
# 3. Snowpark (.to_pandas)
# -----------------------
def snowpark_fetch():
    session = Session.builder.configs(CONN_PARAMS).create()
    df = session.sql(TEST_QUERY).to_pandas()
    session.close()
    return df.shape

# -----------------------
# Run Benchmarks
# -----------------------
if __name__ == "__main__":
    print("Running Snowflake Python read benchmarks...\n")

    elapsed, peak, shape = run_with_memory(default_fetch)
    print(f"Default Connector : {elapsed:.2f}s | peak={peak:.1f} MB | rows={shape[0]}, cols={shape[1]}")

    elapsed, peak, shape = run_with_memory(pandas_fetch)
    print(f"Pandas FetchAll   : {elapsed:.2f}s | peak={peak:.1f} MB | rows={shape[0]}, cols={shape[1]}")
 
    elapsed, peak, shape = run_with_memory(arrow_fetch)
    print(f"Arrow Connector   : {elapsed:.2f}s | peak={peak:.1f} MB | rows={shape[0]}, cols={shape[1]}")

    elapsed, peak, shape = run_with_memory(snowpark_fetch)
    print(f"Snowpark          : {elapsed:.2f}s | peak={peak:.1f} MB | rows={shape[0]}, cols={shape[1]}")
