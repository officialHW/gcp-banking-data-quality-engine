"""
Shared fixtures for all PyTest data quality tests.
conftest.py is automatically loaded by PyTest before any tests run.
"""
import pytest
import pandas as pd
from google.cloud import bigquery


@pytest.fixture(scope="session")
def bq_client():
    """
    BigQuery client shared across all tests.
    scope="session" means this is created once and reused, not reconnecting for every test.
    """
    return bigquery.Client()


@pytest.fixture(scope="session")
def raw_transactions_df(bq_client):
    """
    Load raw transactions from BigQuery into a pandas DataFrame.
    This is what all our data quality tests will run against.
    """
    query = """
        SELECT *
        FROM `banking-dq-engine-490011.banking_dq.raw_transactions`
    """
    print("\nLoading transactions from BigQuery...")
    df = bq_client.query(query).to_dataframe()
    print(f"Loaded {len(df)} rows for testing")
    return df