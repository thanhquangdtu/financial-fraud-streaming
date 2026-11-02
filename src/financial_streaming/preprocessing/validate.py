
import csv
from pathlib import Path

from .common import ROOT, COLUMNS, write_json


# ==============================================================================
# Hàm: check_csv
# - Mục đích: Kiểm định cấu trúc schema và tính toàn vẹn dữ liệu của một file CSV đơn lẻ.
# - Điều cần chú ý:
#   + Kiểm tra header: file bắt buộc phải chứa đầy đủ tất cả các cột trong `COLUMNS`.
#   + Kiểm tra tính duy nhất (Uniqueness): `transaction_id` không được rỗng và không được trùng lặp.
#   + Kiểm tra trường bắt buộc: `customer_id`, `merchant_id`, `category` không được để trống.
#   + Kiểm tra miền giá trị (Value range):
#     * `amount >= 0`: không chấp nhận số tiền âm.
#     * `step >= 0`: bước thời gian không âm.
#     * `data_source` phải khớp chính xác với tham số `source` mong đợi (ví dụ: 'banksim' hoặc 'synthetic').
#   + Kiểm tra nhãn gian lận: `fraud_label` phải là '0' hoặc '1'.
#     * Nếu `allow_missing_label=True` (áp dụng cho synthetic data), chấp nhận trường này để trống `""`.
#   + Giới hạn gom tối đa 20 lỗi đầu tiên để tránh tràn bộ nhớ khi file có nhiều dòng hỏng.
#   + Trả về dict thống kê: `rows`, `errors`, `step_min`, `step_max`, `fraud_counts`.
# ==============================================================================
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


# ==============================================================================
# Hàm: validate
# - Mục đích: Tiến hành kiểm định chất lượng toàn diện trên toàn bộ các tập dữ liệu đã xử lý.
# - Điều cần chú ý:
#   + Kiểm tra 4 tập BankSim (`banksim_clean`, `train`, `validation`, `test`) và 1 tập `synthetic`.
#   + Kiểm tra tính toàn vẹn phân chia thời gian (Chronological split overlap):
#     Đảm bảo `train.step_max < validation.step_min` và `validation.step_max < test.step_min`.
#     Nếu có trùng lặp step giữa các tập liền kề sẽ phát sinh lỗi `chronological split overlap`.
#   + Kiểm tra bảo toàn dữ liệu (Conservation check):
#     Tổng số dòng của `train + validation + test` bắt buộc phải bằng chính xác số dòng của `banksim_clean`.
#     Nếu không khớp sẽ ghi nhận lỗi `split row counts do not match cleaned dataset`.
#   + Trả về dict báo cáo tổng hợp chất lượng của tất cả các file.
# ==============================================================================
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


# ==============================================================================
# Hàm: main
# - Mục đích: Điểm vào thực thi CLI cho tiến trình kiểm định chất lượng dữ liệu.
# - Điều cần chú ý:
#   + Gọi `validate()` và lưu toàn bộ kết quả kiểm định vào `data/reports/data_quality.json`.
#   + In tổng kết số dòng và số lỗi của từng tập dữ liệu ra console.
#   + Quy tắc chặn lỗi (Gatekeeper / Fail-fast): Nếu có bất kỳ tập nào ghi nhận lỗi (`errors > 0`),
#     chương trình lập tức dừng với `SystemExit`, ngăn chặn việc dùng dữ liệu sai cho các pipeline tiếp theo.
#   + Nếu tất cả hợp lệ, in thông báo `Data validation PASSED`.
# ==============================================================================
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
