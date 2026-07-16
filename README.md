# E-Commerce Data Pipeline 🛒

> End-to-end data pipeline processing 500K+ e-commerce records with real-time streaming, distributed transformation, automated ML experiment tracking, and a live KPI dashboard.

## Architecture

```
Synthetic Data (500K orders, 10K customers, 1K products)
        ↓
Kafka (real-time streaming — 3 topics, 1000 msg/sec)
        ↓
Airflow (daily orchestration — 5-task DAG, @daily schedule)
        ↓
PySpark on Databricks (DataFrame API transformations + Delta tables)
        ↓
Databricks SQL (4 business metric tables)
        ↓
dbt (6 models, 9/9 data quality tests passing)
        ↓
PostgreSQL (operational warehouse sink)
Snowflake (cloud analytics warehouse sink)
        ↓
MLflow (2 XGBoost experiments — R2: 0.998)
        ↓
Streamlit Dashboard (live KPI visualization)
```

## Tech Stack

| Tool | Purpose |
|---|---|
| Apache Kafka | Real-time order streaming — 3 topics, 500K+ messages at 1000 msg/sec |
| Apache Airflow | Pipeline orchestration — daily DAG with 5 sequential tasks |
| PySpark | DataFrame API transformations on Databricks — joins, aggregations, window functions |
| Databricks SQL | Distributed SQL transformation — 4 business metric tables |
| dbt | SQL modeling — 6 models, 9/9 automated data quality tests |
| MLflow | Experiment tracking — 2 XGBoost runs, parameters, metrics, model artifacts |
| PostgreSQL | Operational warehouse sink — 3 business metric tables |
| Snowflake | Cloud analytics warehouse — configured integration, loader in warehouse/snowflake_loader.py |
| Docker | Containerized Kafka, Airflow, PostgreSQL |
| Python | Data generation, pipeline scripts, ML training |
| Streamlit | Live KPI dashboard with Plotly charts |

## Pipeline Components

### 1. Synthetic Data Generator (`data/generate.py`)
Generates realistic e-commerce data:
- 500,000 orders — realistic status distribution (75% completed, 10% returned, 8% cancelled)
- 10,000 customers — regions, age segments, premium status
- 1,000 products — 8 categories, pricing, inventory

### 2. Kafka Streaming (`kafka/`)
- Producer streams all 3 datasets to Kafka topics at 1000 msg/sec
- Consumer validates message flow with offset tracking
- Topics: `ecommerce-orders`, `ecommerce-customers`, `ecommerce-products`
- Docker Compose runs Kafka + Zookeeper with dual listener configuration

### 3. Airflow DAG (`dags/ecommerce_pipeline.py`)
Daily pipeline with 5 sequential tasks:
```
ingest_from_kafka → databricks_transform → dbt_models → load_warehouse → train_model
```
- Schedule: `@daily` — runs automatically at midnight
- Retries: 1 with 2-minute delay
- Tags: ecommerce, pipeline, databricks
- Owner: sai

### 4. PySpark on Databricks (`spark/transform.py` + Databricks notebook)
Real PySpark DataFrame API transformations:
- Multi-table joins: orders × products × customers
- GroupBy aggregations with `F.sum()`, `F.count()`, `F.avg()`
- Conditional aggregations with `F.when()` for revenue vs returns
- Window functions with `rank()` over `lifetime_value` for customer ranking
- Customer segmentation: VIP / High Value / Mid Value / Low Value
- Results written as Delta tables to Databricks

### 5. Databricks SQL Transformations
4 business metric tables:
- `daily_revenue` — revenue by day and category (5,848 rows)
- `customer_clv` — customer lifetime value with rankings
- `product_performance` — revenue, returns, return rate per product
- `regional_sales` — revenue and orders by region

### 6. dbt Models (`dbt_project/`)
**Staging layer (views):**
- `stg_orders` — cleaned orders with `is_completed`, `is_returned` flags
- `stg_customers` — cleaned customers with age segments (Gen Z/Millennial/Gen X/Boomer)
- `stg_products` — cleaned products with `gross_margin` and `margin_rate`

**Mart layer (tables):**
- `mart_daily_revenue` — business-ready daily revenue metrics
- `mart_customer_segments` — VIP/High/Mid/Low value segmentation
- `mart_product_performance` — product KPIs with return rates

**Data quality tests: 9/9 passing**
- `unique` — order_id, customer_id, product_id
- `not_null` — all primary keys and totals
- `accepted_values` — order status must be completed/returned/cancelled/pending

### 7. MLflow Experiment Tracking (`mlflow/train.py`)
XGBoost sales forecasting model with 2 experiment runs:

| Run | n_estimators | max_depth | learning_rate | MAE | RMSE | R2 |
|---|---|---|---|---|---|---|
| xgboost_baseline | 100 | 4 | 0.10 | 216.24 | 319.77 | 0.9981 |
| xgboost_tuned | 300 | 6 | 0.05 | 221.17 | 346.38 | 0.9978 |

Baseline slightly outperformed tuned — common with XGBoost on clean, well-structured datasets.

Features: day_of_week, day_of_month, month, quarter, is_weekend, category_encoded, order_count, avg_order_value

### 8. PostgreSQL Warehouse (`warehouse/loader.py`)
Loads 3 dbt mart tables into PostgreSQL:
- `daily_revenue` — 5,848 rows
- `customer_segments` — 10,000 rows
- `product_performance` — 1,000 rows

Uses `ON CONFLICT DO UPDATE` for idempotent loads — safe to run multiple times.

### 9. Snowflake Integration (`warehouse/snowflake_loader.py`)
Cloud analytics warehouse configured as secondary sink:
- Database: `ECOMMERCE_DB`
- Schema: `PUBLIC`
- Tables: `DAILY_REVENUE`, `CUSTOMER_SEGMENTS`, `PRODUCT_PERFORMANCE`
- Uses `write_pandas()` for bulk loading via Snowflake Python connector

### 10. Streamlit Dashboard (`dashboard/app.py`)
Live KPI visualization pulling from PostgreSQL:

**KPI Cards:**
- Total Revenue: $232M+
- Total Orders: 365K+
- Avg Order Value: $636
- Total Customers: 10,000
- Avg Return Rate: 10%

**Charts:**
- Monthly revenue by category (line chart)
- Customer segment distribution (pie chart)
- Top 10 products by revenue (bar chart)
- Return rate by category (color-coded bar chart)
- Customer segments by region (stacked bar chart)
- MLflow model performance comparison

## Setup

```bash
git clone https://github.com/sarvagna23/Ecommerce-pipeline
cd Ecommerce-pipeline

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# fill in Databricks, Snowflake credentials

# Start Docker services
docker-compose up -d

# Run pipeline
python3 data/generate.py
python3 kafka/producer.py
python3 spark/transform.py

# dbt transformations
cd dbt_project && dbt run && dbt test && cd ..

# Load warehouses
python3 warehouse/loader.py
python3 warehouse/snowflake_loader.py

# Train model
python3 mlflow/train.py

# Launch dashboard
streamlit run dashboard/app.py

# View MLflow UI
mlflow ui --port 5001 --backend-store-uri sqlite:///mlflow.db
```

## Environment Variables

```
# Databricks
DATABRICKS_HOST=your-workspace-url
DATABRICKS_TOKEN=your-token
DATABRICKS_WAREHOUSE_ID=your-warehouse-id

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Snowflake
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=ECOMMERCE_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

## Key Technical Decisions

**Why Kafka over direct file ingestion?**
Kafka decouples producers from consumers — the pipeline can process orders in real time as they arrive, not just in batch. Fault tolerant — messages persist even if the consumer goes down.

**Why dbt on top of Databricks SQL?**
dbt adds version control, dependency tracking, and automated data quality tests to raw SQL. Without dbt, transformations are just scripts with no lineage or testing. With dbt, every model has documented dependencies and quality guarantees.

**Why two warehouse sinks (PostgreSQL + Snowflake)?**
PostgreSQL for operational queries and the Streamlit dashboard — low latency, OLTP-optimized. Snowflake for analytical queries at scale — columnar storage, auto-scaling compute, BI tool integration.

**Why baseline outperformed tuned XGBoost?**
The dataset is clean and well-structured with strong temporal patterns. More trees with a lower learning rate (tuned) can overfit on small, clean datasets. Baseline with 100 trees was sufficient to capture the signal.

## Author

**Sai Sarvagna Beeram**
MS Computer Science — Georgia State University (Dec 2026)
GitHub: [sarvagna23](https://github.com/sarvagna23)
LinkedIn: [saisarvagna023](https://linkedin.com/in/saisarvagna023)
