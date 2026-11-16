"""Spark side of Week 5: score every streamed transaction with the exported model (no ML library needed)."""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from financial_streaming.ml.sqlexpr import feature_exprs, score_sql


def load_model(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ==============================================================================
# Hàm: score_transactions
# - Mục đích: Thêm cột fraud_score (0..1), fraud_prediction (0/1), model_version vào luồng giao dịch.
# - Điều cần chú ý:
#   + Đặc trưng chỉ lấy từ chính giao dịch (amount, category, age_group, gender, merchant_id);
#     `fraud_label` KHÔNG được dùng.
#   + Mô hình là các cây GradientBoosting đã xuất ra JSON, chuyển thành biểu thức SQL (ml/sqlexpr.py),
#     nên Spark không cần cài thêm thư viện ML.
#   + Ngưỡng `threshold` được chọn trên tập validation lúc huấn luyện (ml/train.py).
# ==============================================================================
def score_transactions(df, model: dict):
    feats = feature_exprs(model["features"])
    feat_cols = [F.expr(expr).alias(name) for name, expr in feats]
    out = df.select("*", *feat_cols)
    out = out.withColumn("fraud_score", F.round(F.expr(score_sql(model)), 6))
    out = out.drop(*[name for name, _ in feats])
    out = out.withColumn("fraud_prediction", (F.col("fraud_score") >= F.lit(model["threshold"])).cast("int"))
    return out.withColumn("model_version", F.lit(model.get("meta", {}).get("version", "gbt-1")))
