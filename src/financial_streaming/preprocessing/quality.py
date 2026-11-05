"""Additional data-quality checks required by DATA_CONTRACT section 7.

Kept separate from validate.py so the original checks stay untouched. `validate.validate()` merges the
result of `extra_checks` into each file report (keys: null_counts, amount, customers,
step_order_violations) and appends any errors.
"""
import csv
import math
from collections import Counter
from pathlib import Path

from .common import COLUMNS

# ingestion_time is intentionally empty in offline files (assigned by the producer at ingestion).
NEVER_REQUIRED = {"ingestion_time"}


def extra_checks(path, source, allow_missing_label=False, check_step_order=True):
    """Return {"errors": [...], "stats": {...}} for one processed/synthetic CSV.

    - null/empty values in every required column (fraud_label may be empty for synthetic data)
    - amount range: min, max, mean, zero count; non-finite amounts are errors
    - customer transaction counts: unique customers, min/max/mean transactions per customer
    - step ordering: rows must be non-decreasing in `step` (BankSim files only; the synthetic file is
      grouped per customer so global ordering is not required)
    """
    required = [
        c for c in COLUMNS
        if c not in NEVER_REQUIRED and not (allow_missing_label and c == "fraud_label")
    ]
    nulls = Counter()
    customers = Counter()
    amount_n = zero = 0
    amount_sum = 0.0
    amount_min = amount_max = None
    prev_step = None
    order_violations = 0
    first_order_line = None
    non_finite = 0

    with Path(path).open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            for c in required:
                if not (row.get(c) or "").strip():
                    nulls[c] += 1
            customers[row.get("customer_id", "")] += 1
            try:
                amount = float(row["amount"])
                step = int(row["step"])
            except (ValueError, TypeError, KeyError):
                continue  # type/format errors are reported by validate.check_csv
            if not math.isfinite(amount):
                non_finite += 1
                continue
            amount_n += 1
            amount_sum += amount
            zero += amount == 0
            amount_min = amount if amount_min is None else min(amount_min, amount)
            amount_max = amount if amount_max is None else max(amount_max, amount)
            if prev_step is not None and step < prev_step:
                order_violations += 1
                first_order_line = first_order_line or i
            prev_step = step

    errors = []
    for c, n in nulls.items():
        errors.append(f"{n} empty value(s) in required column '{c}'")
    if non_finite:
        errors.append(f"{non_finite} non-finite amount(s)")
    if check_step_order and order_violations:
        errors.append(f"{order_violations} step ordering violation(s), first at line {first_order_line}")

    counts = list(customers.values())
    stats = {
        "null_counts": dict(nulls),
        "amount": {
            "min": amount_min,
            "max": amount_max,
            "mean": round(amount_sum / amount_n, 4) if amount_n else None,
            "zero_count": zero,
        },
        "customers": {
            "unique": len(counts),
            "tx_min": min(counts) if counts else None,
            "tx_max": max(counts) if counts else None,
            "tx_mean": round(sum(counts) / len(counts), 2) if counts else None,
        },
        "step_order_violations": order_violations,
    }
    return {"errors": errors, "stats": stats}

