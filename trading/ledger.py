"""Lossless, side-independent accounting for CCXT ledger snapshots.

CCXT amount is a magnitude, not a signed PnL. Gate's raw info.change is
signed, and its raw pnl/fund types distinguish price PnL from funding even
when CCXT maps them to trade/fee. Unknown legacy signs MUST NOT become wins.
"""

from __future__ import annotations

import math
from typing import Any


def _finite(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def normalize_ledger_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Return a copy retaining evidence plus an explicit signed_amount.

    Safe to apply repeatedly to raw CCXT rows and already-normalized snapshots.
    A legacy row with before/after can be recovered only when its balance delta
    agrees with the magnitude. Old synthetic 0/0 balances are not evidence.
    """
    row = dict(entry)
    info = row.get("info") if isinstance(row.get("info"), dict) else {}
    amount = _finite(row.get("amount"))
    direction = str(row.get("direction") or "").lower()
    signed = None
    source = "unresolved"

    change = _finite(info.get("change"))
    if change is not None:
        signed, source = change, "raw_change"
    elif direction in {"in", "out"} and amount is not None:
        signed = abs(amount) * (1 if direction == "in" else -1)
        source = "direction"
    elif row.get("sign_resolved") is True and _finite(row.get("signed_amount")) is not None:
        signed, source = _finite(row["signed_amount"]), str(row.get("sign_source") or "signed_amount")
    elif amount is not None and amount < 0:
        signed, source = amount, "negative_amount"
    elif amount == 0:
        signed, source = 0.0, "zero"
    else:
        before, after = _finite(row.get("before")), _finite(row.get("after"))
        if before is not None and after is not None and amount is not None:
            delta = after - before
            if delta != 0 and math.isclose(abs(delta), abs(amount), rel_tol=1e-7, abs_tol=1e-9):
                signed, source = delta, "balance_delta"

    row.update({
        "ledger_schema_version": 1,
        "raw_type": row.get("raw_type") or info.get("type") or row.get("type"),
        "signed_amount": signed,
        "sign_resolved": signed is not None,
        "sign_source": source,
    })
    return row


def ledger_category(entry: dict[str, Any]) -> str:
    """Classify without treating every trade/settlement/transfer as profit."""
    info = entry.get("info") if isinstance(entry.get("info"), dict) else {}
    kind = str(entry.get("raw_type") or info.get("type") or entry.get("type") or "").lower()
    if kind in {"pnl", "profit", "realized_pnl", "realised_pnl", "realized", "realised"}:
        return "pnl"
    if kind in {"fund", "funding", "funding_fee", "funding_rate"}:
        return "funding"
    if kind in {"fee", "order_fee", "trade_fee", "trading_fee"}:
        return "fee"
    if kind in {"trade", "settlement", "settle", "set"}:
        # Old Gate snapshots may have lost their original pnl type. Do not
        # guess; surface incomplete accounting until fetched again.
        return "unclassified"
    return "other"
