"""Small, local-first banking transaction quality pipeline.

The same validation functions can be reused in Beam/Dataflow jobs.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    account_id: str
    amount: Decimal
    currency: str
    status: str


REQUIRED_CURRENCIES = {"GBP", "USD", "EUR", "NGN"}
REQUIRED_STATUSES = {"SUCCESS", "FAILED", "PENDING"}


def validate_transaction(tx: Transaction) -> list[str]:
    errors: list[str] = []
    if not tx.transaction_id.strip():
        errors.append("missing_transaction_id")
    if not tx.account_id.strip():
        errors.append("missing_account_id")
    if tx.amount <= 0:
        errors.append("non_positive_amount")
    if tx.currency not in REQUIRED_CURRENCIES:
        errors.append("invalid_currency")
    if tx.status not in REQUIRED_STATUSES:
        errors.append("invalid_status")
    return errors


def quality_report(transactions: Iterable[Transaction]) -> dict[str, int | float]:
    rows = list(transactions)
    invalid = sum(bool(validate_transaction(tx)) for tx in rows)
    total = len(rows)
    return {
        "total_rows": total,
        "valid_rows": total - invalid,
        "invalid_rows": invalid,
        "quality_rate": round((total - invalid) / total, 4) if total else 0.0,
    }
