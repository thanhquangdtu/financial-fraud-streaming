
import argparse
import random
from datetime import datetime, timedelta, timezone

from .common import ROOT, write_csv, write_json, txid


STEP_HOURS = 24


# ==============================================================================
# Hàm: generate
# - Mục đích: Sinh dữ liệu hồ sơ khách hàng và chuỗi giao dịch nhân tạo (synthetic) theo các kịch bản hành vi kiểm soát được.
# - Điều cần chú ý:
#   + Tính tái lập (Reproducibility): Nhận `seed` cố định để đảm bảo dữ liệu sinh ra giống hệt nhau ở mỗi lần chạy.
#   + Ràng buộc: `per_customer` phải >= 10 (mặc định 40 giao dịch/khách hàng).
#   + Xây dựng 4 hồ sơ khách hàng đại diện cho 4 pattern hành vi thực tế:
#     * `SYN_A` (frequent_small): Giao dịch nhỏ đều đặn; 5 giao dịch cuối đột biến số tiền x20.
#     * `SYN_B` (infrequent_high): Giao dịch lớn thưa thớt; 5 giao dịch cuối dồn dập từng step liên tiếp.
#     * `SYN_C` (category_focused): Tập trung ngành đồ ăn; 5 giao dịch cuối nhảy sang ngành du lịch ('es_travel').
#     * `SYN_D` (stable_then_burst): Chi tiêu ổn định; 5 giao dịch cuối xảy ra đồng thời trong cùng 1 step.
#   + Gắn cờ `scenario_anomaly` (0 hoặc 1) để nhận diện bất thường theo kịch bản.
#   + ĐẶC BIỆT CHÚ Ý: Cột `fraud_label` để trống `""` và `data_source = "synthetic"`.
#     `scenario_anomaly` chỉ phục vụ thử nghiệm phân tích hành vi khách hàng, KHÔNG PHẢI nhãn gian lận thật
#     và không được đem đi train mô hình ML.
#   + `event_time` tính theo `start + timedelta(hours=step * STEP_HOURS)` với `STEP_HOURS = 24`.
#   + Trả về tuple gồm danh sách hồ sơ khách hàng `customers` và danh sách giao dịch `rows`.
# ==============================================================================
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
                    start + timedelta(hours=step * STEP_HOURS)
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


# ==============================================================================
# Hàm: main
# - Mục đích: Điểm vào thực thi CLI cho tiến trình sinh dữ liệu synthetic.
# - Điều cần chú ý:
#   + Nhận tham số `--seed` (mặc định 18) và `--per-customer` (mặc định 40).
#   + Xuất file hồ sơ khách hàng: `data/synthetic/customer_profiles.csv`.
#   + Xuất file giao dịch synthetic: `data/synthetic/transactions.csv`.
#   + Lưu metadata cấu hình (seed, start, step_hours, số lượng) vào `data/reports/synthetic_report.json`.
#   + Báo cáo luôn đính kèm ghi chú khẳng định `scenario_anomaly` không phải nhãn fraud thật.
# ==============================================================================
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
            "start": "2020-01-01T00:00:00+00:00",
            "step_hours": STEP_HOURS,
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
