import os
import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv
import time

load_dotenv()

# --- Databricks connection ---
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


def fetch_training_data() -> pd.DataFrame:
    """Fetch daily revenue data from Databricks for forecasting."""
    print("Fetching training data from Databricks...")
    rows = execute_sql("""
        SELECT
            order_date,
            category,
            daily_revenue,
            order_count,
            avg_order_value
        FROM workspace.dbt_ecommerce.mart_daily_revenue
        ORDER BY order_date, category
    """)

    df = pd.DataFrame(rows, columns=[
        "order_date", "category", "daily_revenue",
        "order_count", "avg_order_value"
    ])

    df["order_date"]     = pd.to_datetime(df["order_date"])
    df["daily_revenue"]  = df["daily_revenue"].astype(float)
    df["order_count"]    = df["order_count"].astype(int)
    df["avg_order_value"] = df["avg_order_value"].astype(float)

    print(f"Fetched {len(df):,} rows")
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create time-based features for forecasting."""
    df = df.copy()
    df["day_of_week"]  = df["order_date"].dt.dayofweek
    df["day_of_month"] = df["order_date"].dt.day
    df["month"]        = df["order_date"].dt.month
    df["quarter"]      = df["order_date"].dt.quarter
    df["is_weekend"]   = (df["day_of_week"] >= 5).astype(int)

    # Encode category
    df["category_encoded"] = pd.Categorical(df["category"]).codes

    return df


def run_training():
    """Train XGBoost sales forecasting model with MLflow tracking."""

    # Set MLflow tracking URI — local server
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("ecommerce-sales-forecasting")

    df = fetch_training_data()
    df = engineer_features(df)

    features = [
        "day_of_week", "day_of_month", "month",
        "quarter", "is_weekend", "category_encoded",
        "order_count", "avg_order_value",
    ]
    target = "daily_revenue"

    X = df[features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # --- Experiment 1: baseline ---
    with mlflow.start_run(run_name="xgboost_baseline"):
        params = {
            "n_estimators": 100,
            "max_depth": 4,
            "learning_rate": 0.1,
            "random_state": 42,
        }
        mlflow.log_params(params)

        model = XGBRegressor(**params)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        mae   = mean_absolute_error(y_test, preds)
        rmse  = np.sqrt(mean_squared_error(y_test, preds))
        r2    = r2_score(y_test, preds)

        mlflow.log_metrics({"mae": mae, "rmse": rmse, "r2": r2})
        mlflow.xgboost.log_model(model, "model")

        print(f"Baseline — MAE: {mae:.2f}, RMSE: {rmse:.2f}, R2: {r2:.4f}")

    # --- Experiment 2: tuned ---
    with mlflow.start_run(run_name="xgboost_tuned"):
        params = {
            "n_estimators": 300,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
        }
        mlflow.log_params(params)

        model = XGBRegressor(**params)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        mae   = mean_absolute_error(y_test, preds)
        rmse  = np.sqrt(mean_squared_error(y_test, preds))
        r2    = r2_score(y_test, preds)

        mlflow.log_metrics({"mae": mae, "rmse": rmse, "r2": r2})
        mlflow.xgboost.log_model(model, "model")

        print(f"Tuned — MAE: {mae:.2f}, RMSE: {rmse:.2f}, R2: {r2:.4f}")

    print("\nMLflow experiment tracking complete.")
    print("View runs at: http://localhost:5000")


if __name__ == "__main__":
    run_training()