"""
Data Quality Tests for Banking Transactions.
Tests are organised by data quality dimension:
- Completeness: are mandatory fields populated?
- Accuracy: are mandatory fields populated?
- Consistency: do related fields agree with each other?
"""
import pytest
import pandas as pd


# Completeness Tests____________________________________________________________

class TestCompleteness:
    """Verify o critical fields are missing."""

    @pytest.mark.parametrize("column", [
        "transaction_id",
        "customer_id",
        "account_id",
        "amount",
        "transaction_timestamp",
    ])
    def test_mandatory_fields_not_null(self, raw_transactions_df, column):
        """Critical fields must never be null."""
        null_count = raw_transactions_df[column].isnull().sum()
        total = len(raw_transactions_df)
        null_pct = (null_count / total) * 100

        assert null_count == 0, (
            f"Column '{column}' has {null_count} null values"
            f"({null_pct:.2f}% of {total} records)"
        )

    def test_minimum_record_count(self, raw_transactions_df):
        """Pipeline should contain a minimum volume of transactions."""
        assert len(raw_transactions_df) >=100, (
            f"Expected at least 100 records, got {len(raw_transactions_df)}"
        )

# Accuracy Tests____________________________________________________________

class TestAccuracy:
    """Verify data values are within valid ranges."""

    def test_amounts_are_positive(self, raw_transactions_df):
        """Transaction amounts must be positive."""
        negative = raw_transactions_df[raw_transactions_df["amount"] < 0]
        assert len(negative) == 0, (
            f"Found {len(negative)} transactions with negative amounts"
        )

    def test_valid_transaction_types(self, raw_transactions_df):
        """Transaction types must be from the allowed set."""
        valid_types = {"DEBIT", "CREDIT", "TRANSFER"}
        actual_types = set(raw_transactions_df["transaction_type"].dropna().unique())
        invalid = actual_types - valid_types

        assert len(invalid) == 0, (
            f"Found invalid transaction types: {invalid}"
        )

    def test_valid_currencies(self, raw_transactions_df):
        """Currencies must be valid ISO codes."""
        valid_currencies = {"GBP", "EUR", "USD", "CHF"}
        actual = set(raw_transactions_df["currency"].dropna().unique())
        invalid = actual - valid_currencies

        assert len(invalid) == 0, (
            f"Found invalid currencies: {invalid}"
        )

    def test_no_future_transactions(self, raw_transactions_df):
        """Transactions should not have future timestamps."""
        now = pd.Timestamp.now(tz="UTC")
        timestamps = pd.to_datetime(
            raw_transactions_df["transaction_timestamp"], utc=True
        )
        future = raw_transactions_df[timestamps > now]

        assert len(future) == 0, (
            f"Found {len(future)} transactions with future timestamps"
        )


# Consistency Tests________________________________________________________________________________

class TestConsistency:
    """verify data relationships and referential integrity."""

    def test_no_duplicate_transaction_ids(self, raw_transactions_df):
        """Transaction IDs must be unique."""
        dupes = raw_transactions_df[
            raw_transactions_df["transaction_id"].duplicated()
        ]
        assert len(dupes) == 0, (
            f"Found {len(dupes)} duplicated transaction IDs"
        )
    
    def test_channel_device_consistency(self, raw_transactions_df):
        """ATM device type should only appear on ATM channel."""
        atm_device_wrong_channel = raw_transactions_df[
            (raw_transactions_df["device_type"] == "ATM") &
            (raw_transactions_df["channel"] != "ATM")
        ]
        assert len(atm_device_wrong_channel) == 0, (
            f"Found {len(atm_device_wrong_channel)} ATM devices on non-ATM channels"
        )