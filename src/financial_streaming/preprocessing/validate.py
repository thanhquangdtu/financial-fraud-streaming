
import csv
from pathlib import Path

from .common import ROOT, COLUMNS, write_json


def check_csv(path, source, allow_missing_label=False):
    errors = []
    seen = set()
    count = 0
    steps = []
    labels = {"0": 0, "1": 0}

    with Path(path).open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames or not set(COLUMNS).issubset(
            reader.fieldnames
        ):
            return {
                "rows": 0,
                "errors": ["missing required columns"],
            }

        for i, r in enumerate(reader, start=2):
            count += 1

            try:
                if not r["transaction_id"] or r["transaction_id"] in seen:
                    raise ValueError(
                        "missing or duplicate transaction_id"
                    )

                seen.add(r["transaction_id"])

                if (
                    not r["customer_id"]
                    or not r["merchant_id"]
                    or not r["category"]
                ):
                    raise ValueError("missing identifier/category")

                if float(r["amount"]) < 0:
                    raise ValueError("negative amount")

                s = int(r["step"])
                steps.append(s)

                if s < 0:
                    raise ValueError("negative step")

                if r["data_source"] != source:
                    raise ValueError("wrong data_source")

                label = r["fraud_label"]

                if label not in ("0", "1") and not (
                    allow_missing_label and label == ""
                ):
                    raise ValueError("invalid fraud label")

                if label in labels:
                    labels[label] += 1

            except (ValueError, TypeError) as e:
                if len(errors) < 20:
                    errors.append(f"line {i}: {e}")

    return {
        "rows": count,
        "errors": errors,
        "step_min": min(steps) if steps else None,
        "step_max": max(steps) if steps else None,
        "fraud_counts": labels,
    }


def validate():
    reports = {}

    for name in (
        "banksim_clean",
        "train",
        "validation",
        "test",
    ):
        path = ROOT / f"data/processed/{name}.csv"

        reports[name] = check_csv(
            path,
            "banksim",
        )

    reports["synthetic"] = check_csv(
        ROOT / "data/synthetic/transactions.csv",
        "synthetic",
        True,
    )

    splits = [
        reports[n]
        for n in ("train", "validation", "test")
    ]

    for left, right in zip(splits, splits[1:]):
        if (
            left["step_max"] is not None
            and right["step_min"] is not None
            and left["step_max"] >= right["step_min"]
        ):
            right["errors"].append(
                "chronological split overlap"
            )

    if sum(s["rows"] for s in splits) != reports["banksim_clean"]["rows"]:
        reports["banksim_clean"]["errors"].append(
            "split row counts do not match cleaned dataset"
        )

    return reports


def main():
    report = validate()

    write_json(
        ROOT / "data/reports/data_quality.json",
        report,
    )

    for name, result in report.items():
        print(
            name,
            result["rows"],
            "rows",
            len(result["errors"]),
            "errors",
        )

    if any(r["errors"] for r in report.values()):
        raise SystemExit(
            "Data validation failed; "
            "inspect data/reports/data_quality.json"
        )

    print("Data validation PASSED")


if __name__ == "__main__":
    main()
