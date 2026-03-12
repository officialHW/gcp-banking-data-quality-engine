"""
Step definitions for transaction data quality BDD tests.
Each function maps to a plain English line in the feature file.
"""
import pandas as pd
from behave import given, when, then
from google.cloud import bigquery


@given("the raw transactions table is loaded with recent data")
def step_load_transactions(context):
    """Load transactions from BigQuery into context so all steps can use it."""
    client = bigquery.Client()
    query = """
        SELECT *
        FROM `banking-dq-engine-490011.banking_dq.raw_transactions`
    """
    context.df = client.query(query).to_dataframe()
    assert len(context.df) > 0, "No data found in raw_transactions"


@when("I check for null values in mandatory fields")
def step_check_nulls(context):
    """Calculate null counts for all columns."""
    context.null_counts = context.df.isnull().sum().to_dict()


@then("the following fields should have zero nulls")
def step_verify_no_nulls(context):
    """Verify specified fields have no nulls."""
    for row in context.table:
        field = row["field"]
        null_count = context.null_counts.get(field, 0)
        assert null_count == 0, (
            f"Field '{field}' has {null_count} null values"
        )


@when("I check the amount field")
def step_check_amounts(context):
    """Store amount series for validation."""
    context.amounts = context.df["amount"].dropna()


@then("all amounts should be greater than {value:d}")
def step_amounts_greater_than(context, value):
    """Verify all amounts exceed the minimum."""
    violations = context.amounts[context.amounts <= value]
    assert len(violations) == 0, (
        f"Found {len(violations)} amounts <= {value}"
    )


@when("I check the currency field")
def step_check_currency(context):
    """Store unique currencies found in the data."""
    context.currencies = set(context.df["currency"].dropna().unique())


@then("all values should be one of")
def step_valid_currencies(context):
    """Verify all currencies are in the allowed set."""
    valid = {row["currency"] for row in context.table}
    invalid = context.currencies - valid
    assert len(invalid) == 0, (
        f"Found invalid currencies: {invalid}"
    )


@when("I check for duplicate transaction_id values")
def step_check_duplicates(context):
    """Count duplicate transaction IDs."""
    context.duplicate_count = int(
        context.df["transaction_id"].duplicated().sum()
    )


@then("the duplicate count should be {count:d}")
def step_verify_duplicate_count(context, count):
    """Verify expected duplicate count."""
    assert context.duplicate_count == count, (
        f"Expected {count} duplicates, found {context.duplicate_count}"
    )


@when("I count all transactions")
def step_count_transactions(context):
    """Count total transactions."""
    context.total_count = len(context.df)


@then("there should be at least {count:d} transactions")
def step_minimum_count(context, count):
    """Verify minimum transaction count."""
    assert context.total_count >= count, (
        f"Expected >= {count} transactions, got {context.total_count}"
    )