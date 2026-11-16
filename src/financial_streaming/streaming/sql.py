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
def build_upsert(target, staging, columns, keys, update, update_columns=None):
    for k in keys:
        if k not in columns:
            raise ValueError(f"key {k} is not in columns")
    cols = ", ".join(columns)
    conflict = ", ".join(keys)
    values = update_columns if update_columns is not None else [c for c in columns if c not in keys]
    if update and values:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in values)
        action = f"DO UPDATE SET {sets}"
    else:
        action = "DO NOTHING"
    return (f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging} "
            f"ON CONFLICT ({conflict}) {action}")


# Week 5: scores are added to `transactions`; idempotent migration for databases created in Week 4.
# Also drop `stg_transactions` so Spark JDBC (`truncate=true`) recreates the staging table with the new columns.
MIGRATION_SQL = (
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fraud_score DOUBLE PRECISION, "
    "ADD COLUMN IF NOT EXISTS fraud_prediction INT, "
    "ADD COLUMN IF NOT EXISTS model_version VARCHAR(32); "
    "DROP TABLE IF EXISTS stg_transactions"
)

TABLES = {
    "transactions": {
        "columns": ["transaction_id", "customer_id", "merchant_id", "step", "event_time", "ingestion_time",
                    "amount", "category", "age_group", "gender", "zipcode_ori", "zipcode_merchant",
                    "fraud_label", "data_source", "schema_version",
                    "fraud_score", "fraud_prediction", "model_version"],
        "keys": ["transaction_id"],
        # The event itself never changes; only the model output may be refreshed (e.g. after retraining).
        "update": True,
        "update_columns": ["fraud_score", "fraud_prediction", "model_version"],
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
    return build_upsert(name, f"stg_{name}", t["columns"], t["keys"], t["update"], t.get("update_columns"))

