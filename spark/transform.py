import os
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv
load_dotenv()
import time

# --- Databricks connection ---
client = WorkspaceClient(
    host  = os.getenv("DATABRICKS_HOST"),
    token = os.getenv("DATABRICKS_TOKEN"),
)

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
DATA_DIR     = os.path.join(os.path.dirname(__file__), "../data/generated")


def execute_sql(sql: str) -> list:
    """Execute SQL on Databricks SQL Warehouse and return results."""
    response = client.statement_execution.execute_statement(
        warehouse_id = WAREHOUSE_ID,
        statement    = sql,
        wait_timeout = "30s",
    )
    
    # Wait for completion
    while response.status.state in [StatementState.PENDING, StatementState.RUNNING]:
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)
    
    if response.status.state != StatementState.SUCCEEDED:
        raise Exception(f"SQL failed: {response.status.error}")
    
    return response.result.data_array or []


def setup_tables():
    """Create database and tables in Databricks."""
    print("Setting up Databricks tables...")
    
    execute_sql("CREATE DATABASE IF NOT EXISTS ecommerce")
    
    execute_sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.orders (
            order_id    STRING,
            customer_id STRING,
            product_id  STRING,
            quantity    INT,
            unit_price  DOUBLE,
            discount    DOUBLE,
            total       DOUBLE,
            order_date  TIMESTAMP,
            status      STRING
        )
    """)
    
    execute_sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.customers (
            customer_id STRING,
            age         INT,
            region      STRING,
            signup_date DATE,
            is_premium  BOOLEAN
        )
    """)
    
    execute_sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.products (
            product_id  STRING,
            category    STRING,
            base_price  DOUBLE,
            cost        DOUBLE,
            inventory   INT
        )
    """)
    
    print("Tables created successfully")


def load_data_to_databricks():
    """Load CSV data into Databricks tables using SQL INSERT."""
    print("Loading data into Databricks...")
    
    # Load orders in batches of 1000
    orders = pd.read_csv(f"{DATA_DIR}/orders.csv")
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    products = pd.read_csv(f"{DATA_DIR}/products.csv")
    
    # Load customers
    print(f"Loading {len(customers):,} customers...")
    for i in range(0, len(customers), 1000):
        batch = customers.iloc[i:i+1000]
        values = ",".join([
            f"('{r.customer_id}', {r.age}, '{r.region}', '{r.signup_date}', {str(r.is_premium).lower()})"
            for _, r in batch.iterrows()
        ])
        execute_sql(f"INSERT INTO ecommerce.customers VALUES {values}")
        if (i + 1000) % 5000 == 0:
            print(f"  {i+1000:,} customers loaded")
    
    # Load products
    print(f"Loading {len(products):,} products...")
    for i in range(0, len(products), 100):
        batch = products.iloc[i:i+100]
        values = ",".join([
            f"('{r.product_id}', '{r.category}', {r.base_price}, {r.cost}, {r.inventory})"
            for _, r in batch.iterrows()
        ])
        execute_sql(f"INSERT INTO ecommerce.products VALUES {values}")
    
    # Load orders in batches
    print(f"Loading {len(orders):,} orders...")
    for i in range(0, len(orders), 1000):
        batch = orders.iloc[i:i+1000]
        values = ",".join([
            f"('{r.order_id}', '{r.customer_id}', '{r.product_id}', {r.quantity}, {r.unit_price}, {r.discount}, {r.total}, '{r.order_date}', '{r.status}')"
            for _, r in batch.iterrows()
        ])
        execute_sql(f"INSERT INTO ecommerce.orders VALUES {values}")
        if (i + 1000) % 50000 == 0:
            print(f"  {i+1000:,} orders loaded")
    
    print("All data loaded to Databricks")


def run_transformations():
    """Run PySpark-style transformations on Databricks."""
    print("Running transformations on Databricks...")
    
    # Transformation 1 — Daily revenue by category
    print("  Computing daily revenue...")
    execute_sql("""
        CREATE OR REPLACE TABLE ecommerce.daily_revenue AS
        SELECT
            DATE(order_date)        AS order_date,
            p.category,
            SUM(o.total)            AS daily_revenue,
            COUNT(o.order_id)       AS order_count,
            AVG(o.total)            AS avg_order_value
        FROM ecommerce.orders o
        JOIN ecommerce.products p ON o.product_id = p.product_id
        WHERE o.status = 'completed'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    
    # Transformation 2 — Customer lifetime value
    print("  Computing customer lifetime value...")
    execute_sql("""
        CREATE OR REPLACE TABLE ecommerce.customer_clv AS
        SELECT
            o.customer_id,
            c.region,
            c.is_premium,
            SUM(o.total)            AS lifetime_value,
            COUNT(o.order_id)       AS total_orders,
            AVG(o.total)            AS avg_order_value,
            MIN(o.order_date)       AS first_order,
            MAX(o.order_date)       AS last_order
        FROM ecommerce.orders o
        JOIN ecommerce.customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'completed'
        GROUP BY 1, 2, 3
        ORDER BY lifetime_value DESC
    """)
    
    # Transformation 3 — Product performance
    print("  Computing product performance...")
    execute_sql("""
        CREATE OR REPLACE TABLE ecommerce.product_performance AS
        SELECT
            o.product_id,
            p.category,
            SUM(CASE WHEN o.status = 'completed' THEN o.total ELSE 0 END) AS revenue,
            COUNT(o.order_id)                                               AS total_orders,
            SUM(CASE WHEN o.status = 'returned'  THEN 1 ELSE 0 END)       AS returns,
            ROUND(
                SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) * 1.0
                / COUNT(o.order_id), 3
            )                                                               AS return_rate
        FROM ecommerce.orders o
        JOIN ecommerce.products p ON o.product_id = p.product_id
        GROUP BY 1, 2
        ORDER BY revenue DESC
    """)
    
    # Transformation 4 — Regional sales
    print("  Computing regional sales...")
    execute_sql("""
        CREATE OR REPLACE TABLE ecommerce.regional_sales AS
        SELECT
            c.region,
            SUM(o.total)                AS total_revenue,
            COUNT(o.order_id)           AS total_orders,
            COUNT(DISTINCT o.customer_id) AS unique_customers
        FROM ecommerce.orders o
        JOIN ecommerce.customers c ON o.customer_id = c.customer_id
        WHERE o.status = 'completed'
        GROUP BY 1
        ORDER BY total_revenue DESC
    """)
    
    print("All transformations complete")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    setup_tables()
    load_data_to_databricks()
    run_transformations()
    
    print("\nDatabricks transformation pipeline complete!")