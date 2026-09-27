from decimal import Decimal

from src.pipeline import Transaction, quality_report, validate_transaction


def test_valid_transaction_has_no_errors():
    tx = Transaction("TX001", "ACC001", Decimal("125.50"), "GBP", "SUCCESS")
    assert validate_transaction(tx) == []


def test_invalid_transaction_is_flagged():
    tx = Transaction("", "ACC001", Decimal("-10"), "ABC", "UNKNOWN")
    errors = validate_transaction(tx)
    assert set(errors) == {
        "missing_transaction_id",
        "non_positive_amount",
        "invalid_currency",
        "invalid_status",
    }


def test_quality_report():
    rows = [
        Transaction("TX001", "ACC001", Decimal("10"), "GBP", "SUCCESS"),
        Transaction("TX002", "ACC002", Decimal("20"), "GBP", "SUCCESS"),
        Transaction("TX003", "ACC003", Decimal("0"), "GBP", "SUCCESS"),
    ]
    report = quality_report(rows)
    assert report["total_rows"] == 3
    assert report["invalid_rows"] == 1
    assert report["quality_rate"] == 0.6667
