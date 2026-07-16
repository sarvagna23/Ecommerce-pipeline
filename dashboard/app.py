import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(
    page_title="E-Commerce Pipeline Dashboard",
    page_icon="🛒",
    layout="wide"
)

st.title("E-Commerce Data Pipeline Dashboard")
st.caption("Powered by Kafka · Airflow · Databricks · dbt · MLflow · PostgreSQL")


def get_conn():
    return psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST", "localhost"),
        port     = os.getenv("POSTGRES_PORT", "5432"),
        dbname   = os.getenv("POSTGRES_DB", "ecommerce"),
        user     = os.getenv("POSTGRES_USER", "postgres"),
        password = os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


@st.cache_data(ttl=60)
def load_daily_revenue():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM daily_revenue ORDER BY order_date", conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_customer_segments():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM customer_segments", conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_product_performance():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM product_performance ORDER BY revenue DESC", conn)
    conn.close()
    return df


# --- Load data ---
revenue_df  = load_daily_revenue()
customer_df = load_customer_segments()
product_df  = load_product_performance()

# --- KPI cards ---
st.divider()
st.subheader("Pipeline KPIs")

total_revenue    = revenue_df["daily_revenue"].sum()
total_orders     = revenue_df["order_count"].sum()
avg_order_value  = revenue_df["avg_order_value"].mean()
total_customers  = len(customer_df)
vip_customers    = len(customer_df[customer_df["customer_segment"] == "VIP"])
avg_return_rate  = product_df["return_rate"].mean()

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Revenue",    f"${total_revenue:,.0f}")
c2.metric("Total Orders",     f"{total_orders:,}")
c3.metric("Avg Order Value",  f"${avg_order_value:.2f}")
c4.metric("Total Customers",  f"{total_customers:,}")
c5.metric("VIP Customers",    f"{vip_customers:,}")
c6.metric("Avg Return Rate",  f"{avg_return_rate:.1%}")

st.divider()

# --- Revenue trend ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Revenue by Category")
    revenue_df["order_date"] = pd.to_datetime(revenue_df["order_date"])
    monthly = revenue_df.copy()
    monthly["month"] = monthly["order_date"].dt.to_period("M").astype(str)
    monthly_agg = monthly.groupby(["month", "category"])["daily_revenue"].sum().reset_index()
    fig = px.line(
        monthly_agg,
        x="month", y="daily_revenue", color="category",
        title="Monthly Revenue by Category",
        labels={"daily_revenue": "Revenue ($)", "month": "Month"},
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Customer Segments")
    seg_counts = customer_df["customer_segment"].value_counts().reset_index()
    seg_counts.columns = ["segment", "count"]
    fig2 = px.pie(
        seg_counts, values="count", names="segment",
        title="Customer Segment Distribution",
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# --- Product performance ---
col3, col4 = st.columns(2)

with col3:
    st.subheader("Top 10 Products by Revenue")
    top10 = product_df.head(10)
    fig3 = px.bar(
        top10, x="product_id", y="revenue",
        color="category",
        title="Top 10 Products by Revenue",
        labels={"revenue": "Revenue ($)"},
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("Return Rate by Category")
    cat_returns = product_df.groupby("category")["return_rate"].mean().reset_index()
    fig4 = px.bar(
        cat_returns, x="category", y="return_rate",
        title="Average Return Rate by Category",
        labels={"return_rate": "Return Rate"},
        color="return_rate",
        color_continuous_scale="RdYlGn_r",
    )
    st.plotly_chart(fig4, use_container_width=True)

st.divider()

# --- Regional sales ---
st.subheader("Customer Segments by Region")
region_seg = customer_df.groupby(["region", "customer_segment"]).size().reset_index(name="count")
fig5 = px.bar(
    region_seg, x="region", y="count", color="customer_segment",
    title="Customer Segments by Region",
    barmode="stack",
)
st.plotly_chart(fig5, use_container_width=True)

st.divider()

# --- MLflow metrics ---
st.subheader("MLflow Model Performance")
ml_col1, ml_col2 = st.columns(2)
with ml_col1:
    st.markdown("**XGBoost Baseline**")
    st.metric("MAE",  "216.24")
    st.metric("RMSE", "319.77")
    st.metric("R2",   "0.9981")
with ml_col2:
    st.markdown("**XGBoost Tuned**")
    st.metric("MAE",  "221.17")
    st.metric("RMSE", "346.38")
    st.metric("R2",   "0.9978")

st.caption("Baseline model performed slightly better — common with XGBoost on small, clean datasets.")