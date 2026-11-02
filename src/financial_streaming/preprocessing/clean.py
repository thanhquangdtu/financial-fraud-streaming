
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


# ==============================================================================
# Hàm: normalize
# - Mục đích: Chuẩn hóa một dòng bản ghi từ dữ liệu thô (BankSim raw) thành schema sự kiện chuẩn.
# - Điều cần chú ý:
#   + Kiểm tra tính đầy đủ: tất cả các trường trong `RAW_COLUMNS` đều phải có giá trị sau khi clean_str.
#   + Ràng buộc nghiệp vụ:
#     * `step >= 0`: bước thời gian mô phỏng không âm.
#     * `amount >= 0`: giá trị giao dịch không được âm (chấp nhận 0.0 theo dữ liệu BankSim).
#     * `fraud` phải là 0 hoặc 1.
#     * `customer`, `merchant`, `category` không được để trống.
#   + Tính toán thời gian sự kiện: `event_time = start + timedelta(hours=step * step_hours)` dạng ISO 8601.
#   + Trường `ingestion_time` để trống `""` vì đây là bước xử lý offline (sẽ do Kafka Producer gán ở tuần 2).
#   + Sinh `transaction_id` tất định qua hàm `txid`.
#   + Gán nhãn nguồn `data_source = "banksim"`.
#   + Ném `ValueError` nếu vi phạm bất kỳ điều kiện nào để hàm `clean()` bắt và ghi nhận dòng lỗi.
# ==============================================================================
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


# ==============================================================================
# Hàm: clean
# - Mục đích: Xử lý làm sạch toàn bộ file CSV BankSim thô, phân tách dòng hợp lệ và không hợp lệ.
# - Điều cần chú ý:
#   + Kiểm tra `start` timestamp phải có múi giờ (timezone-aware) và `step_hours` phải dương (> 0).
#   + Mở file nguồn với encoding `utf-8-sig` để tự động xử lý ký tự BOM nếu có.
#   + Kiểm tra header file nguồn: phải chứa toàn bộ tập cột `RAW_COLUMNS`.
#   + Cơ chế chịu lỗi (Fault-tolerance): Không làm gián đoạn chương trình khi gặp dòng hỏng;
#     các dòng lỗi (`ValueError`, `TypeError`, `OverflowError`) được gom lại kèm số dòng và nguyên nhân
#     để ghi ra file `invalid_output` (Dead-letter audit log).
#   + Dòng hợp lệ được ghi ra file `output`.
#   + Trả về dict báo cáo chứa `start`, `step_hours`, số dòng hợp lệ/không hợp lệ và đường dẫn file.
# ==============================================================================
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
        "start": start.isoformat(),
        "step_hours": step_hours,
        "valid_rows": len(valid),
        "invalid_rows": len(bad),
        "output": str(output),
        "invalid_output": str(invalid_output),
    }


# ==============================================================================
# Hàm: main
# - Mục đích: Điểm vào thực thi CLI cho tiến trình làm sạch dữ liệu BankSim.
# - Điều cần chú ý:
#   + Nhận các tham số dòng lệnh qua argparse:
#     * `--input`: đường dẫn file CSV raw (tùy chọn, mặc định lấy trong data/raw).
#     * `--start`: mốc thời gian ISO bắt đầu mô phỏng (mặc định '2020-01-01T00:00:00+00:00').
#     * `--step-hours`: số giờ cho mỗi bước `step` (mặc định 24.0 giờ).
#   + Xuất file đã làm sạch vào `data/processed/banksim_clean.csv`.
#   + Xuất dòng lỗi vào `data/reports/invalid_rows.csv`.
#   + Lưu kết quả thống kê vào `data/reports/preprocessing.json`.
# ==============================================================================
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
