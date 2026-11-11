"""SQL builders for the PostgreSQL sinks (no pyspark import, so they can be unit tested locally)."""


# ==============================================================================
# Hàm: build_upsert
# - Mục đích: Sinh câu lệnh INSERT ... ON CONFLICT chuyển dữ liệu từ bảng staging sang bảng đích.
# - Điều cần chú ý:
#   + `update=True`: khi trùng khóa thì GHI ĐÈ các cột giá trị (dùng cho bảng thống kê theo cửa sổ,
#     vì Spark (mode update) luôn gửi giá trị mới nhất của cửa sổ).
#   + `update=False`: khi trùng khóa thì BỎ QUA (DO NOTHING) - dùng cho bảng transactions.
#   + Chạy lại cùng một batch nhiều lần cho cùng kết quả (idempotent).
# ==============================================================================
def build_upsert(target, staging, columns, keys, update):
    for k in keys:
        if k not in columns:
            raise ValueError(f"key {k} is not in columns")
    cols = ", ".join(columns)
    conflict = ", ".join(keys)
    values = [c for c in columns if c not in keys]
    if update and values:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in values)
        action = f"DO UPDATE SET {sets}"
    else:
        action = "DO NOTHING"
    return (f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging} "
            f"ON CONFLICT ({conflict}) {action}")


TABLES = {
    "transactions": {
        "columns": ["transaction_id", "customer_id", "merchant_id", "step", "event_time", "ingestion_time",
                    "amount", "category", "age_group", "gender", "zipcode_ori", "zipcode_merchant",
                    "fraud_label", "data_source", "schema_version"],
        "keys": ["transaction_id"],
        "update": False,
    },
    "daily_stats": {
        "columns": ["window_start", "window_end", "category", "tx_count", "total_amount", "avg_amount"],
        "keys": ["window_start", "category"],
        "update": True,
    },
    "customer_week": {
        "columns": ["window_start", "window_end", "customer_id", "tx_count", "total_amount", "avg_amount",
                    "distinct_categories"],
        "keys": ["window_start", "customer_id"],
        "update": True,
    },
}

DEAD_LETTER_UPSERT = ("INSERT INTO dead_letter (id, raw) SELECT md5(raw), raw FROM stg_dead_letter "
                      "ON CONFLICT (id) DO NOTHING")


def upsert_sql(name):
    t = TABLES[name]
    return build_upsert(name, f"stg_{name}", t["columns"], t["keys"], t["update"])

