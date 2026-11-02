
import argparse
import csv
from collections import Counter

from .common import ROOT, COLUMNS, write_csv, write_json


# ==============================================================================
# Hàm: split
# - Mục đích: Chia tập dữ liệu thành 3 tập Train, Validation, Test theo thứ tự thời gian (chronological split).
# - Điều cần chú ý:
#   + Ràng buộc tham số: `train_ratio > 0`, `val_ratio > 0` và tổng `train_ratio + val_ratio < 1`.
#   + Bắt buộc dữ liệu phải có tối thiểu 3 giá trị `step` phân biệt.
#   + Cơ chế phân chia theo thời gian: Sắp xếp các `step` duy nhất và cắt ngưỡng theo danh sách step đó.
#     * Tập train nhận các step đầu tiên `[:a]`.
#     * Tập validation nhận các step tiếp theo `[a:b]`.
#     * Tập test nhận các step cuối cùng `[b:]`.
#   + TUYỆT ĐỐI KHÔNG chia ngẫu nhiên (random shuffle) để ngăn ngừa rò rỉ thông tin tương lai (data leakage) trong bài toán streaming/time-series.
#   + Trả về dict gồm 3 danh sách bản ghi: `train`, `validation`, `test`.
# ==============================================================================
def split(rows, train_ratio=0.7, val_ratio=0.15):
    if (
        not 0 < train_ratio < 1
        or not 0 < val_ratio < 1
        or train_ratio + val_ratio >= 1
    ):
        raise ValueError("invalid ratios")

    steps = sorted({int(r["step"]) for r in rows})

    if len(steps) < 3:
        raise ValueError(
            "At least 3 distinct steps are required "
            "for chronological split"
        )

    n = len(steps)

    a = max(1, min(n - 2, int(n * train_ratio)))
    b = max(
        a + 1,
        min(n - 1, int(n * (train_ratio + val_ratio))),
    )

    train_steps = set(steps[:a])
    val_steps = set(steps[a:b])
    test_steps = set(steps[b:])

    parts = {
        "train": [],
        "validation": [],
        "test": [],
    }

    for row in rows:
        s = int(row["step"])

        if s in train_steps:
            name = "train"
        elif s in val_steps:
            name = "validation"
        else:
            name = "test"

        parts[name].append(row)

    return parts


# ==============================================================================
# Hàm: main
# - Mục đích: Điểm vào thực thi CLI cho tiến trình phân chia tập dữ liệu.
# - Điều cần chú ý:
#   + Nhận tham số dòng lệnh `--train-ratio` (mặc định 0.7) và `--validation-ratio` (mặc định 0.15).
#   + Nguồn dữ liệu đầu vào: đọc file đã làm sạch `data/processed/banksim_clean.csv`.
#   + Xuất 3 tập dữ liệu tương ứng: `train.csv`, `validation.csv`, `test.csv` trong `data/processed/`.
#   + Thống kê số lượng dòng, số lượng fraud (0 và 1), dải step (`step_min`, `step_max`)
#     và lưu vào `data/reports/split_report.json`.
#   + Kiểm tra và phát cảnh báo nếu một tập không có mẫu gian lận nào (`fraud_1 == 0`).
# ==============================================================================
def main():
    p = argparse.ArgumentParser()

    p.add_argument(
        "--train-ratio",
        type=float,
        default=0.7,
    )
    p.add_argument(
        "--validation-ratio",
        type=float,
        default=0.15,
    )

    a = p.parse_args()

    source = ROOT / "data/processed/banksim_clean.csv"

    with source.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    parts = split(
        rows,
        a.train_ratio,
        a.validation_ratio,
    )

    report = {}

    for name, records in parts.items():
        write_csv(
            ROOT / f"data/processed/{name}.csv",
            records,
        )

        labels = Counter(
            str(r["fraud_label"]) for r in records
        )

        report[name] = {
            "rows": len(records),
            "fraud_0": labels["0"],
            "fraud_1": labels["1"],
            "step_min": min(
                (int(r["step"]) for r in records),
                default=None,
            ),
            "step_max": max(
                (int(r["step"]) for r in records),
                default=None,
            ),
        }

    write_json(
        ROOT / "data/reports/split_report.json",
        report,
    )

    print(report)

    if any(x["fraud_1"] == 0 for x in report.values()):
        print(
            "WARNING: At least one split has no fraud examples. "
            "Review dataset/split boundaries."
        )


if __name__ == "__main__":
    main()
