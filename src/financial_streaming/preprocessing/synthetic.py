
import argparse
import random
from datetime import datetime, timedelta, timezone

from .common import ROOT, write_csv, write_json, txid


def generate(seed=18, per_customer=40):
    if per_customer < 10:
        raise ValueError("per-customer must be >= 10")

    rng = random.Random(seed)

    profiles = [
        ("SYN_A", "frequent_small", 15, "es_transportation"),
        ("SYN_B", "infrequent_high", 350, "es_hotelservices"),
        ("SYN_C", "category_focused", 50, "es_food"),
        ("SYN_D", "stable_then_burst", 70, "es_sportsandtoys"),
    ]

    customers = []
    rows = []
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)

    for customer, behavior, base, category in profiles:
        customers.append({
            "customer_id": customer,
            "behavior": behavior,
            "baseline_amount": base,
            "primary_category": category,
            "data_source": "synthetic",
        })

        for i in range(per_customer):
            unusual = i >= per_customer - 5

            amount = round(
                max(0.01, rng.uniform(base * 0.8, base * 1.2)),
                2,
            )

            cat = category
            step = i * (3 if behavior == "infrequent_high" else 1)

            if unusual and behavior == "frequent_small":
                amount = round(base * 20, 2)

            if unusual and behavior == "infrequent_high":
                step = (
                    (per_customer - 5) * 3
                    + (i - (per_customer - 5))
                )

            if unusual and behavior == "category_focused":
                cat = "es_travel"

            if unusual and behavior == "stable_then_burst":
                step = per_customer - 5

            rows.append({
                "transaction_id": txid(
                    "synthetic-" + customer, i
                ),
                "customer_id": customer,
                "merchant_id": "SYN_M_" + cat,
                "step": step,
                "event_time": (
                    start + timedelta(hours=step)
                ).isoformat(),
                "ingestion_time": "",
                "amount": amount,
                "category": cat,
                "age_group": "3",
                "gender": "U",
                "zipcode_ori": "00000",
                "zipcode_merchant": "00000",
                "fraud_label": "",
                "data_source": "synthetic",
                "scenario_anomaly": int(unusual),
            })

    return customers, rows


def main():
    p = argparse.ArgumentParser()

    p.add_argument(
        "--seed",
        type=int,
        default=18,
    )
    p.add_argument(
        "--per-customer",
        type=int,
        default=40,
    )

    a = p.parse_args()

    profiles, rows = generate(
        a.seed,
        a.per_customer,
    )

    write_csv(
        ROOT / "data/synthetic/customer_profiles.csv",
        profiles,
        list(profiles[0]),
    )

    write_csv(
        ROOT / "data/synthetic/transactions.csv",
        rows,
        list(rows[0]),
    )

    write_json(
        ROOT / "data/reports/synthetic_report.json",
        {
            "seed": a.seed,
            "customers": len(profiles),
            "transactions": len(rows),
            "note": (
                "scenario_anomaly is not a confirmed fraud label"
            ),
        },
    )

    print(
        f"Generated {len(rows)} synthetic transactions "
        f"for {len(profiles)} customers"
    )


if __name__ == "__main__":
    main()
