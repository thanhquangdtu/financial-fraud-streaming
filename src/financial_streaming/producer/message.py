import json

SCHEMA_VERSION = "1.0"
REQUIRED = ("transaction_id", "customer_id", "merchant_id", "category", "data_source")


# ==============================================================================
# Hàm: row_to_message
# - Mục đích: Chuyển đổi một dòng dữ liệu CSV (từ processed hoặc synthetic) thành message sự kiện chuẩn JSON.
# - Điều cần chú ý:
#   + Ràng buộc bắt buộc: Phải có đủ các trường trong `REQUIRED` (transaction_id, customer_id, merchant_id, category, data_source).
#   + Kiểm tra tính hợp lệ: `amount >= 0`, `step >= 0`, `fraud_label` thuộc ("", "0", "1").
#   + Ràng buộc thời gian:
#     * `event_time`: Giữ NGUYÊN vẹn giá trị gốc đã tiền xử lý.
#     * `ingestion_time`: Được gán mới tại chính thời điểm phát sự kiện vào Kafka.
#   + Cờ kịch bản thử nghiệm: `scenario_anomaly` (trong file synthetic) BỊ LOẠI BỎ, cố tình không gửi lên Kafka
#     để tránh rò rỉ vào luồng streaming/ML.
#   + Đính kèm `schema_version = "1.0"` theo đúng DATA_CONTRACT phiên bản 1.0.
# ==============================================================================
def row_to_message(row, ingestion_time):
    """Convert one CSV row (processed/synthetic) to the normalized event of DATA_CONTRACT 2.

    `event_time` is kept exactly as stored in the data. `ingestion_time` is assigned here,
    at ingestion. `scenario_anomaly` (synthetic only) is deliberately NOT sent.
    Raises ValueError for rows that cannot form a valid event.
    """
    for k in REQUIRED:
        if not row.get(k):
            raise ValueError(f"missing {k}")
    amount = float(row["amount"])
    step = int(row["step"])
    if amount < 0 or step < 0:
        raise ValueError("negative amount or step")
    label = row.get("fraud_label", "")
    if label not in ("", "0", "1"):
        raise ValueError("invalid fraud_label")
    if not row.get("event_time"):
        raise ValueError("missing event_time")
    return {
        "transaction_id": row["transaction_id"],
        "customer_id": row["customer_id"],
        "merchant_id": row["merchant_id"],
        "step": step,
        "event_time": row["event_time"],
        "ingestion_time": ingestion_time,
        "amount": amount,
        "category": row["category"],
        "age_group": row.get("age_group", ""),
        "gender": row.get("gender", ""),
        "zipcode_ori": row.get("zipcode_ori", ""),
        "zipcode_merchant": row.get("zipcode_merchant", ""),
        "fraud_label": int(label) if label != "" else None,
        "data_source": row["data_source"],
        "schema_version": SCHEMA_VERSION,
    }


# ==============================================================================
# Hàm: to_key
# - Mục đích: Trích xuất Kafka Message Key và mã hóa thành dạng bytes UTF-8.
# - Điều cần chú ý:
#   + Sử dụng `customer_id` làm key thay vì `transaction_id`.
#   + Ý nghĩa kiến trúc: Đảm bảo toàn bộ các giao dịch của cùng 1 khách hàng sẽ luôn rơi vào
#     CÙNG MỘT PARTITION và duy trì ĐÚNG THỨ TỰ thời gian, phục vụ trực tiếp cho bài toán
#     phân tích hành vi khách hàng (Customer Insights) và trượt cửa sổ (Windowing) ở Tuần 3.
# ==============================================================================
def to_key(message):
    return message["customer_id"].encode("utf-8")  # same customer -> same partition -> per-customer ordering


# ==============================================================================
# Hàm: to_value
# - Mục đích: Tuần tự hóa (serialize) toàn bộ dict payload sự kiện thành chuỗi JSON dạng bytes UTF-8.
# - Điều cần chú ý:
#   + Sử dụng `ensure_ascii=False` để giữ nguyên các ký tự Unicode nếu có.
#   + Trả về `bytes` để gửi trực tiếp qua KafkaProducer.
# ==============================================================================
def to_value(message):
    return json.dumps(message, ensure_ascii=False).encode("utf-8")

