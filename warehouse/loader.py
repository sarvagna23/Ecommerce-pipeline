import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
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


def get_pg_conn():
    return psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST", "localhost"),
        port     = os.getenv("POSTGRES_PORT", "5432"),
        dbname   = os.getenv("POSTGRES_DB", "ecommerce"),
        user     = os.getenv("POSTGRES_USER", "postgres"),
        password = os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def setup_postgres_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_revenue (
            order_date      DATE,
            category        VARCHAR(50),
            daily_revenue   NUMERIC(12,2),
            order_count     INTEGER,
            avg_order_value NUMERIC(10,2),
            PRIMARY KEY (order_date, category)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_segments (
            customer_id      VARCHAR(20) PRIMARY KEY,
            region           VARCHAR(30),
            is_premium       BOOLEAN,
            age_segment      VARCHAR(20),
            lifetime_value   NUMERIC(12,2),
            total_orders     INTEGER,
            avg_order_value  NUMERIC(10,2),
            customer_segment VARCHAR(20)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_performance (
            product_id  VARCHAR(20) PRIMARY KEY,
            category    VARCHAR(50),
            base_price  NUMERIC(10,2),
            margin_rate NUMERIC(5,3),
            revenue     NUMERIC(12,2),
            total_orders INTEGER,
            returns      INTEGER,
            return_rate  NUMERIC(5,3),
            units_sold   INTEGER
        )
    """)

    conn.commit()
    cur.close()
    print("PostgreSQL tables created")


def load_to_postgres():
    conn = get_pg_conn()
    setup_postgres_tables(conn)
    cur = conn.cursor()

    # Load daily revenue
    print("Loading daily revenue...")
    rows = execute_sql("""
    SELECT
        order_date,
        category,
        daily_revenue,
        order_count,
        avg_order_value
    FROM workspace.dbt_ecommerce.mart_daily_revenue
""")
    if rows:
        execute_values(cur, """
            INSERT INTO daily_revenue
            (order_date, category, daily_revenue, order_count, avg_order_value)
            VALUES %s
            ON CONFLICT (order_date, category) DO UPDATE SET
                daily_revenue   = EXCLUDED.daily_revenue,
                order_count     = EXCLUDED.order_count,
                avg_order_value = EXCLUDED.avg_order_value
        """, rows)
        conn.commit()
        print(f"  {len(rows):,} rows loaded")

    # Load customer segments
    print("Loading customer segments...")
    rows = execute_sql("""
        SELECT customer_id, region, is_premium, age_segment,
               lifetime_value, total_orders, avg_order_value, customer_segment
        FROM workspace.dbt_ecommerce.mart_customer_segments
    """)
    if rows:
        execute_values(cur, """
            INSERT INTO customer_segments
            (customer_id, region, is_premium, age_segment,
             lifetime_value, total_orders, avg_order_value, customer_segment)
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                lifetime_value   = EXCLUDED.lifetime_value,
                total_orders     = EXCLUDED.total_orders,
                customer_segment = EXCLUDED.customer_segment
        """, rows)
        conn.commit()
        print(f"  {len(rows):,} rows loaded")

    # Load product performance
    print("Loading product performance...")
    rows = execute_sql("""
        SELECT product_id, category, base_price, margin_rate,
               revenue, total_orders, returns, return_rate, units_sold
        FROM workspace.dbt_ecommerce.mart_product_performance
    """)
    if rows:
        execute_values(cur, """
            INSERT INTO product_performance
            (product_id, category, base_price, margin_rate,
             revenue, total_orders, returns, return_rate, units_sold)
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                revenue      = EXCLUDED.revenue,
                total_orders = EXCLUDED.total_orders,
                return_rate  = EXCLUDED.return_rate
        """, rows)
        conn.commit()
        print(f"  {len(rows):,} rows loaded")

    cur.close()
    conn.close()
    print("\nPostgreSQL warehouse load complete")


if __name__ == "__main__":
    load_to_postgres()