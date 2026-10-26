
import argparse
import csv
from datetime import datetime, timedelta, timezone

from .common import (
    RAW_COLUMNS,
    COLUMNS,
    ROOT,
    clean_str,
    source_file,
    txid,
    write_csv,
    write_json,
)


def normalize(row, source_name, line_number, start, step_hours):
    if any(not clean_str(row.get(k)) for k in RAW_COLUMNS):
        raise ValueError("required value missing")

    step = int(clean_str(row["step"]))
    amount = float(clean_str(row["amount"]))
    fraud = int(clean_str(row["fraud"]))

    if step < 0 or amount < 0 or fraud not in (0, 1):
        raise ValueError("invalid step, amount or fraud label")

    customer = clean_str(row["customer"])
    merchant = clean_str(row["merchant"])
    category = clean_str(row["category"])

    if not customer or not merchant or not category:
        raise ValueError("missing identifier/category")

    return {
        "transaction_id": txid(source_name, line_number),
        "customer_id": customer,
        "merchant_id": merchant,
        "step": step,
        "event_time": (
            start + timedelta(hours=step * step_hours)
        ).isoformat(),
        "ingestion_time": "",
        "amount": amount,
        "category": category,
        "age_group": clean_str(row["age"]),
        "gender": clean_str(row["gender"]),
        "zipcode_ori": clean_str(row["zipcodeOri"]),
        "zipcode_merchant": clean_str(row["zipMerchant"]),
        "fraud_label": fraud,
        "data_source": "banksim",
    }


def clean(
    path,
    output,
    invalid_output,
    start="2020-01-01T00:00:00+00:00",
    step_hours=24,
):
    start = datetime.fromisoformat(start.replace("Z", "+00:00"))

    if start.tzinfo is None:
        raise ValueError("start timestamp must include timezone")

    if step_hours <= 0:
        raise ValueError("step-hours must be positive")

    valid = []
    bad = []

    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames or not set(RAW_COLUMNS).issubset(
            reader.fieldnames
        ):
            raise ValueError("BankSim header invalid")

        for i, row in enumerate(reader, start=2):
            try:
                valid.append(
                    normalize(row, path.name, i, start, step_hours)
                )

            except (ValueError, TypeError, OverflowError) as exc:
                bad.append({
                    "line_number": i,
                    "reason": str(exc),
                    "raw_row": str(row),
                })

    write_csv(output, valid)
    write_csv(
        invalid_output,
        bad,
        ["line_number", "reason", "raw_row"],
    )

    return {
        "valid_rows": len(valid),
        "invalid_rows": len(bad),
        "output": str(output),
        "invalid_output": str(invalid_output),
    }


def main():
    p = argparse.ArgumentParser()

    p.add_argument("--input")
    p.add_argument(
        "--start",
        default="2020-01-01T00:00:00+00:00",
    )
    p.add_argument(
        "--step-hours",
        type=float,
        default=24,
    )

    a = p.parse_args()

    result = clean(
        source_file(a.input),
        ROOT / "data/processed/banksim_clean.csv",
        ROOT / "data/reports/invalid_rows.csv",
        a.start,
        a.step_hours,
    )

    write_json(
        ROOT / "data/reports/preprocessing.json",
        result,
    )

    print(result)


if __name__ == "__main__":
    main()
