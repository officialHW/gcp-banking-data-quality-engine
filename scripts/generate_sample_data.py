"""Generate realistic banking transaction data for testing."""
import json
import random
import uuid
from datetime import datetime, timedelta
from google.cloud import bigquery

client = bigquery.Client()

TRANSACTION_TYPES = ["DEBIT", "CREDIT", "TRANSFER"]
CHANNELS = ["ONLINE", "BRANCH", "ATM", "MOBILE"]
STATUSES = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
CURRENCIES = ["GBP", "EUR", "USD"]
MERCHANTS = [
    ("Tesco", "GROCERY"), ("Amazon", "RETAIL"),
    ("Shell", "FUEL"), ("Costa", "FOOD_DRINK"),
    ("Netflix", "ENTERTAINMENT"), ("TfL", "TRANSPORT"),
]

def generate_transactions(n=1000, include_bad_data=True):
    """Generate n transactions, optionally including quality issues."""
    rows = []
    base_time = datetime.now() - timedelta(days=7)

    for i in range(n):
        merchant, category = random.choice(MERCHANTS)
        row = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "account_id": f"ACC-{random.randint(10000, 99999)}",
            "transaction_type": random.choice(TRANSACTION_TYPES),
            "amount": round(random.uniform(1.50, 5000.0), 2),
            "currency": random.choice(CURRENCIES),
            "merchant_name": merchant,
            "merchant_category": category,
            "transaction_timestamp": (
                base_time + timedelta(minutes=random.randint(0, 10080))
            ).isoformat(),
            "channel": random.choice(CHANNELS),
            "status": random.choices(
                STATUSES, weights = [70,15,10,5]
            )[0],
            "country_code": random.choice(["GB", "IE", "DE", "FR", "US"]),
            "ip_address": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "device_type": random.choice(["iOS", "Android", "Web", "ATM"]),
        }

        # Intentionally inject data quality issues (~10% of records)
        if include_bad_data and random.random() < 0.10:
            issue = random.choice([
                "null_amount", "negative_amount", "future_date",
                "null_customer", "invalid_currency", "duplicate_id",
            ])
            if issue == "null_amount":
                row["amount"] = None
            elif issue == "negative_amount":
                row["amount"] = -abs(row["amount"])
            elif issue == "future_date":
                row["transaction_timestamp"] = (
                    datetime.utcnow() + timedelta(days=30)
                ).isoformat()
            elif issue == "null_customer":
                row["customer_id"] = None
            elif issue == "invalid_currency":
                row["currency"] = "XXX"
            elif issue == "duplicate_id":
                row["transaction_id"] = rows[-1]["transaction_id"] if rows else row["transaction_id"]
        
        rows.append(row)

    return rows

def load_to_bigquery(rows):
    """Load generated rows into BigQuery via DataFrame."""
    import pandas as pd

    df = pd.DataFrame(rows)

    # Convert timestamp strings to proper datetime
    df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"], utc=True)
    df["ingestion_timestamp"] = pd.Timestamp.now(tz="UTC")

    table_id = "banking-dq-engine-490011.banking_dq.raw_transactions"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite existing data
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Loaded {len(df)} rows to {table_id}")

if __name__ == "__main__":
    data = generate_transactions(n=5000, include_bad_data=True)
    load_to_bigquery(data)
    # Also save locally for unit testing
    with open("tests/fixtures/sample_transactions.json", "w") as f:
        json.dump(data[:100], f, indent=2)
    print("Sample fixture saved to tests/fixtures/")
