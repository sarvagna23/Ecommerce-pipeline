import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import os

random.seed(42)
np.random.seed(42)

# --- Config ---
NUM_CUSTOMERS = 10_000
NUM_PRODUCTS  = 1_000
NUM_ORDERS    = 500_000
START_DATE    = datetime(2023, 1, 1)
END_DATE      = datetime(2024, 12, 31)
OUTPUT_DIR    = os.path.join(os.path.dirname(__file__), "generated")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Toys", "Food"]
REGIONS    = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
STATUSES   = ["completed", "returned", "cancelled", "pending"]
STATUS_WEIGHTS = [0.75, 0.10, 0.08, 0.07]


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    random_days    = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    return start + timedelta(days=random_days, seconds=random_seconds)


# ---------------------------------------------------------------------------
# 1. Customers
# ---------------------------------------------------------------------------
print("Generating customers...")
customer_ids = [f"CUST-{i:05d}" for i in range(1, NUM_CUSTOMERS + 1)]
customers = pd.DataFrame({
    "customer_id":   customer_ids,
    "age":           np.random.randint(18, 70, NUM_CUSTOMERS),
    "region":        np.random.choice(REGIONS, NUM_CUSTOMERS),
    "signup_date":   [random_date(START_DATE, END_DATE).date() for _ in range(NUM_CUSTOMERS)],
    "is_premium":    np.random.choice([True, False], NUM_CUSTOMERS, p=[0.2, 0.8]),
})
customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
print(f"  {len(customers):,} customers saved")


# ---------------------------------------------------------------------------
# 2. Products
# ---------------------------------------------------------------------------
print("Generating products...")
product_ids = [f"PROD-{i:04d}" for i in range(1, NUM_PRODUCTS + 1)]
products = pd.DataFrame({
    "product_id":   product_ids,
    "category":     np.random.choice(CATEGORIES, NUM_PRODUCTS),
    "base_price":   np.round(np.random.uniform(5.0, 500.0, NUM_PRODUCTS), 2),
    "cost":         np.round(np.random.uniform(2.0, 200.0, NUM_PRODUCTS), 2),
    "inventory":    np.random.randint(0, 1000, NUM_PRODUCTS),
})
products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
print(f"  {len(products):,} products saved")


# ---------------------------------------------------------------------------
# 3. Orders
# ---------------------------------------------------------------------------
print("Generating orders... (this takes ~30 seconds)")

# Pre-sample for speed
sampled_customers = np.random.choice(customer_ids, NUM_ORDERS)
sampled_products  = np.random.choice(product_ids,  NUM_ORDERS)
product_price_map = dict(zip(products["product_id"], products["base_price"]))

order_records = []
for i in range(NUM_ORDERS):
    cust_id    = sampled_customers[i]
    prod_id    = sampled_products[i]
    quantity   = random.randint(1, 5)
    unit_price = product_price_map[prod_id]
    discount   = round(random.uniform(0, 0.3), 2)
    total      = round(unit_price * quantity * (1 - discount), 2)
    order_date = random_date(START_DATE, END_DATE)
    status     = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]

    order_records.append({
        "order_id":    f"ORD-{i+1:07d}",
        "customer_id": cust_id,
        "product_id":  prod_id,
        "quantity":    quantity,
        "unit_price":  unit_price,
        "discount":    discount,
        "total":       total,
        "order_date":  order_date,
        "status":      status,
    })

    if (i + 1) % 100_000 == 0:
        print(f"  {i+1:,} / {NUM_ORDERS:,} orders generated")

orders = pd.DataFrame(order_records)
orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
print(f"  {len(orders):,} orders saved")

print("\nDone. Files saved to data/generated/")
print(f"  customers.csv — {len(customers):,} rows")
print(f"  products.csv  — {len(products):,} rows")
print(f"  orders.csv    — {len(orders):,} rows")