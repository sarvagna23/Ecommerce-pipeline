import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv
import time

load_dotenv()

client = WorkspaceClient(
    host  = os.getenv("DATABRICKS_HOST"),
    token = os.getenv("DATABRICKS_TOKEN"),
)
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")


def execute_sql(sql: str) -> list:
    response = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    while response.status.state in [StatementState.PENDING, StatementState.RUNNING]:
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise Exception(f"SQL failed: {response.status.error}")
    return response.result.data_array or []


def get_snowflake_conn():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DB"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        ocsp_response_cache_filename="/tmp/ocsp_cache",
        insecure_mode=True,
    )


def setup_snowflake():
    """Create database, schema, and tables in Snowflake."""
    conn = get_snowflake_conn()
    cur  = conn.cursor()

    cur.execute("CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB")
    cur.execute("USE DATABASE ECOMMERCE_DB")
    cur.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
    cur.execute("USE SCHEMA PUBLIC")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS DAILY_REVENUE (
            ORDER_DATE      DATE,
            CATEGORY        VARCHAR(50),
            DAILY_REVENUE   FLOAT,
            ORDER_COUNT     INTEGER,
            AVG_ORDER_VALUE FLOAT,
            PRIMARY KEY (ORDER_DATE, CATEGORY)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS CUSTOMER_SEGMENTS (
            CUSTOMER_ID      VARCHAR(20) PRIMARY KEY,
            REGION           VARCHAR(30),
            IS_PREMIUM       BOOLEAN,
            AGE_SEGMENT      VARCHAR(20),
            LIFETIME_VALUE   FLOAT,
            TOTAL_ORDERS     INTEGER,
            AVG_ORDER_VALUE  FLOAT,
            CUSTOMER_SEGMENT VARCHAR(20)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS PRODUCT_PERFORMANCE (
            PRODUCT_ID   VARCHAR(20) PRIMARY KEY,
            CATEGORY     VARCHAR(50),
            BASE_PRICE   FLOAT,
            MARGIN_RATE  FLOAT,
            REVENUE      FLOAT,
            TOTAL_ORDERS INTEGER,
            RETURNS      INTEGER,
            RETURN_RATE  FLOAT,
            UNITS_SOLD   INTEGER
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Snowflake tables created successfully")


def load_to_snowflake():
    """Load dbt mart tables from Databricks into Snowflake."""
    setup_snowflake()
    conn = get_snowflake_conn()
    cur  = conn.cursor()
    cur.execute("USE DATABASE ECOMMERCE_DB")
    cur.execute("USE SCHEMA PUBLIC")

    # Load daily revenue
    print("Loading daily revenue to Snowflake...")
    rows = execute_sql("""
        SELECT order_date, category, daily_revenue, order_count, avg_order_value
        FROM workspace.dbt_ecommerce.mart_daily_revenue
    """)
    df = pd.DataFrame(rows, columns=[
        "ORDER_DATE", "CATEGORY", "DAILY_REVENUE", "ORDER_COUNT", "AVG_ORDER_VALUE"
    ])
    cur.execute("TRUNCATE TABLE IF EXISTS DAILY_REVENUE")
    write_pandas(conn, df, "DAILY_REVENUE")
    print(f"  {len(df):,} rows loaded")

    # Load customer segments
    print("Loading customer segments to Snowflake...")
    rows = execute_sql("""
        SELECT customer_id, region, is_premium, age_segment,
               lifetime_value, total_orders, avg_order_value, customer_segment
        FROM workspace.dbt_ecommerce.mart_customer_segments
    """)
    df = pd.DataFrame(rows, columns=[
        "CUSTOMER_ID", "REGION", "IS_PREMIUM", "AGE_SEGMENT",
        "LIFETIME_VALUE", "TOTAL_ORDERS", "AVG_ORDER_VALUE", "CUSTOMER_SEGMENT"
    ])
    cur.execute("TRUNCATE TABLE IF EXISTS CUSTOMER_SEGMENTS")
    write_pandas(conn, df, "CUSTOMER_SEGMENTS")
    print(f"  {len(df):,} rows loaded")

    # Load product performance
    print("Loading product performance to Snowflake...")
    rows = execute_sql("""
        SELECT product_id, category, base_price, margin_rate,
               revenue, total_orders, returns, return_rate, units_sold
        FROM workspace.dbt_ecommerce.mart_product_performance
    """)
    df = pd.DataFrame(rows, columns=[
        "PRODUCT_ID", "CATEGORY", "BASE_PRICE", "MARGIN_RATE",
        "REVENUE", "TOTAL_ORDERS", "RETURNS", "RETURN_RATE", "UNITS_SOLD"
    ])
    cur.execute("TRUNCATE TABLE IF EXISTS PRODUCT_PERFORMANCE")
    write_pandas(conn, df, "PRODUCT_PERFORMANCE")
    print(f"  {len(df):,} rows loaded")

    cur.close()
    conn.close()
    print("\nSnowflake warehouse load complete")


if __name__ == "__main__":
    load_to_snowflake()