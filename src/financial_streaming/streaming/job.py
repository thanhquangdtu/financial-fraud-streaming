"""Week 3 - Spark Structured Streaming job: Kafka -> parse/validate -> dedup -> event-time windows.

Runs inside the `spark` docker compose service (see docker-compose.yml), e.g.:
    docker compose run --rm spark /workspace/src/financial_streaming/streaming/job.py            # live demo
    docker compose run --rm spark /workspace/src/financial_streaming/streaming/job.py --check    # verify

Pipeline
  1. read `transactions.raw` (schema 1.0 JSON) from Kafka
  2. parse + validate; invalid messages go to a dead-letter stream (not dropped silently)
  3. drop duplicates by (transaction_id, event_time) inside the watermark (producer replay is at-least-once)
  4. event-time aggregations (the only source of results is Spark; nothing is fabricated):
       daily_stats    : 1-day tumbling window x category  -> count, total, average amount
       customer_week  : 7-day tumbling window x customer  -> count, total, average, distinct categories
Not in this week: Parquet/PostgreSQL storage and production checkpoint/recovery tests (Week 4), ML (Week 5).
The fraud label is not used by any aggregation.
"""
import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# The job is started as a plain script by spark-submit, so make the package importable (src/).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from financial_streaming.streaming.sql import DEAD_LETTER_UPSERT, upsert_sql  # noqa: E402

SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("step", T.IntegerType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("ingestion_time", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("category", T.StringType()),
    T.StructField("age_group", T.StringType()),
    T.StructField("gender", T.StringType()),
    T.StructField("zipcode_ori", T.StringType()),
    T.StructField("zipcode_merchant", T.StringType()),
    T.StructField("fraud_label", T.IntegerType()),
    T.StructField("data_source", T.StringType()),
    T.StructField("schema_version", T.StringType()),
])
REQUIRED = ["transaction_id", "customer_id", "merchant_id", "step", "category", "data_source", "amount"]

# Spark aligns windows to the Unix epoch (a Thursday). The simulation starts on 2020-01-01, which is
# 18262 days after the epoch (18262 mod 7 = 6), so 7-day windows are shifted by 6 days to start on step 0.
WEEK_START_OFFSET = "6 days"


# ==============================================================================
# Hàm: parse
# - Mục đích: Phân tích cú pháp chuỗi JSON thô từ Kafka, kiểm định schema và phân luồng dữ liệu.
# - Điều cần chú ý:
#   + Tách DataFrame thành 2 luồng: `valid` (hợp lệ) và `dead` (Dead-Letter Queue).
#   + Dòng hợp lệ phải thỏa mãn: không null ở các trường bắt buộc (`REQUIRED`), `amount >= 0`,
#     `step >= 0`, `schema_version == '1.0'`, và định dạng timestamp của `event_time` hợp lệ.
#   + Chuyển đổi kiểu dữ liệu `event_time` và `ingestion_time` sang dạng TimestampType.
# ==============================================================================
def parse(raw):
    """raw: DataFrame with a binary `value` column. Returns (valid, dead_letter)."""
    df = raw.select(F.col("value").cast("string").alias("raw"))
    df = df.withColumn("m", F.from_json("raw", SCHEMA))
    df = df.withColumn("event_ts", F.to_timestamp(F.col("m.event_time")))
    ok = F.col("m").isNotNull() & F.col("event_ts").isNotNull()
    for c in REQUIRED:
        ok = ok & F.col(f"m.{c}").isNotNull()
    ok = ok & (F.col("m.amount") >= 0) & (F.col("m.step") >= 0) & (F.col("m.schema_version") == "1.0")
    ok = F.coalesce(ok, F.lit(False))
    cols = [F.col(f"m.{c}").alias(c) for c in SCHEMA.fieldNames() if c not in ("event_time", "ingestion_time")]
    valid = df.filter(ok).select(
        *cols,
        F.col("event_ts").alias("event_time"),
        F.to_timestamp(F.col("m.ingestion_time")).alias("ingestion_time"),
    )
    dead = df.filter(~ok).select("raw")
    return valid, dead


# ==============================================================================
# Hàm: dedup
# - Mục đích: Khử trùng lặp giao dịch (Deduplication) trên luồng dữ liệu thời gian thực.
# - Điều cần chú ý:
#   + Áp dụng `withWatermark("event_time", watermark)` (mặc định 1 ngày) để giới hạn kích thước state bộ nhớ.
#   + Khử trùng lặp dựa trên cặp khóa `["transaction_id", "event_time"]`.
#   + Đảm bảo tính Idempotent khi Kafka Producer phát lại (At-least-once replay).
# ==============================================================================
def dedup(valid, watermark):
    # Same transaction replayed -> same transaction_id and same event_time -> removed; state is bounded
    # by the watermark.
    return valid.withWatermark("event_time", watermark).dropDuplicates(["transaction_id", "event_time"])


# ==============================================================================
# Hàm: daily_stats
# - Mục đích: Tính toán tổng hợp giao dịch theo cửa sổ trượt 1 ngày (1-day tumbling window) và ngành hàng.
# - Điều cần chú ý:
#   + Gom nhóm theo `window("event_time", "1 day")` và `category`.
#   + Đo đạc các chỉ số: số lượng giao dịch (`tx_count`), tổng tiền (`total_amount`), trung bình (`avg_amount`).
#   + Chuẩn hóa cột kết quả gồm: `window_start`, `window_end`, `category`, `tx_count`, `total_amount`, `avg_amount`.
# ==============================================================================
def daily_stats(df):
    return (
        df.groupBy(F.window("event_time", "1 day").alias("w"), "category")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 4).alias("avg_amount"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "category", "tx_count", "total_amount", "avg_amount")
    )


# ==============================================================================
# Hàm: customer_week
# - Mục đích: Phân tích hành vi khách hàng theo cửa sổ trượt 7 ngày (7-day tumbling window).
# - Điều cần chú ý:
#   + Sử dụng `WEEK_START_OFFSET = "6 days"` để căn chỉnh cửa sổ tuần trùng khớp với step 0 (2020-01-01).
#   + Gom nhóm theo `window("event_time", "7 days", ...)` và `customer_id`.
#   + Đo lường: `tx_count`, `total_amount`, `avg_amount`, và số lượng ngành hàng khác nhau (`distinct_categories`).
#   + Đóng vai trò là đặc trưng hành vi quan trọng cho mô hình ML ở Tuần 5.
# ==============================================================================
def customer_week(df):
    return (
        df.groupBy(F.window("event_time", "7 days", "7 days", WEEK_START_OFFSET).alias("w"), "customer_id")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 4).alias("avg_amount"),
            F.size(F.collect_set("category")).alias("distinct_categories"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "customer_id", "tx_count", "total_amount", "avg_amount", "distinct_categories")
    )


# ==============================================================================
# Hàm: compare
# - Mục đích: So sánh đối chiếu kết quả giữa Streaming và Batch recomputation phục vụ nghiệm thu.
# - Điều cần chú ý:
#   + Thực hiện Full Outer Join giữa bảng streaming và bảng batch theo danh sách khóa `keys`.
#   + Kiểm tra sai lệch số dòng và sai số giá trị `abs(s - b) > 1e-6` trên các cột giá trị `vals`.
#   + Trả về dict: `stream_rows`, `batch_rows`, `mismatched_rows`.
# ==============================================================================
def compare(name, stream_df, batch_df, keys, vals):
    """Full outer join of the streaming result with a batch recomputation; count mismatching rows."""
    s = stream_df.select(*keys, *[F.col(v).alias("s_" + v) for v in vals])
    b = batch_df.select(*keys, *[F.col(v).alias("b_" + v) for v in vals])
    j = s.join(b, keys, "full_outer")
    bad = F.lit(False)
    for v in vals:
        sv, bv = F.col("s_" + v), F.col("b_" + v)
        bad = bad | sv.isNull() | bv.isNull() | (F.abs(sv - bv) > 1e-6)
    j = j.cache()
    result = {"stream_rows": stream_df.count(), "batch_rows": batch_df.count(),
              "mismatched_rows": j.filter(bad).count()}
    print(name, result)
    return result


# ==============================================================================
# Hàm: kafka_reader
# - Mục đích: Khởi tạo DataFrame đọc dữ liệu từ Kafka (hỗ trợ cả Streaming và Batch).
# - Điều cần chú ý:
#   + Nếu `streaming=True`: Sử dụng `spark.readStream`.
#   + Nếu `streaming=False`: Sử dụng `spark.read` với `endingOffsets="latest"` để đọc batch toàn bộ topic.
#   + Cấu hình `failOnDataLoss="false"` để đảm bảo an toàn khi Kafka dọn dẹp log quá hạn.
# ==============================================================================
def kafka_reader(spark, args, streaming):
    r = spark.readStream if streaming else spark.read
    r = (r.format("kafka")
         .option("kafka.bootstrap.servers", args.bootstrap)
         .option("subscribe", args.topic)
         .option("startingOffsets", args.starting_offsets)
         .option("failOnDataLoss", "false"))
    if not streaming:
        r = r.option("endingOffsets", "latest")
    return r.load()


# ==============================================================================
# Hàm: run_live
# - Mục đích: Thực thi job Spark Streaming trực tiếp (Production / Live mode).
# - Điều cần chú ý:
#   + Đọc stream từ K      00000000000000000000000000000afka, phân tích schema, loại bỏ dữ liệu hỏng vào `dead_letter`.
#   + Khử trùng lặp qua `dedup`.
#   + Xuất luồng `daily_stats` (update mode), `customer_week` (update mode), và `dead_letter` (append mode).
#   + Cấu hình checkpoint định kỳ cho từng luồng tại `args.checkpoint_dir`.
# ==============================================================================
def run_sql(spark, args, sql):
    """Execute one SQL statement on PostgreSQL through the JVM JDBC driver (Spark's own JDBC writer
    cannot do INSERT ... ON CONFLICT)."""
    jvm = spark.sparkContext._gateway.jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver = jvm.java.lang.Class.forName("org.postgresql.Driver", True, loader).newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", args.pg_user)
    props.setProperty("password", args.pg_password)
    conn = driver.connect(args.pg_url, props)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


# ==============================================================================
# Hàm: make_pg_writer
# - Mục đích: Tạo hàm cho `foreachBatch` ghi mỗi micro-batch vào PostgreSQL một cách IDEMPOTENT.
# - Điều cần chú ý:
#   + Bước 1: ghi batch vào bảng tạm `stg_<name>` (JDBC, overwrite + truncate).
#   + Bước 2: chạy `INSERT ... ON CONFLICT` (xem `streaming/sql.py`) từ bảng tạm sang bảng đích.
#   + Nếu Spark chạy lại batch sau sự cố, kết quả trong DB không đổi (không trùng dòng).
# ==============================================================================
def make_pg_writer(spark, args, name, upsert):
    def write(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        (batch_df.write.format("jdbc").option("url", args.pg_url).option("dbtable", f"stg_{name}")
         .option("user", args.pg_user).option("password", args.pg_password)
         .option("driver", "org.postgresql.Driver").option("truncate", "true").mode("overwrite").save())
        run_sql(spark, args, upsert)
    return write


# ==============================================================================
# Hàm: run_live
# - Mục đích: Chạy job Spark Streaming trực tiếp, lưu kết quả bền vững (Tuần 4).
# - Điều cần chú ý:
#   + `transactions` (sự kiện đã khử trùng) -> PostgreSQL (DO NOTHING) và Parquet partition theo ngày.
#   + `daily_stats`, `customer_week` (update mode) -> PostgreSQL (ghi đè giá trị mới nhất của cửa sổ).
#   + `dead_letter` -> PostgreSQL (id = md5(raw)).
#   + Mỗi query có checkpoint riêng trong `args.checkpoint_dir`; `--console` in thêm ra màn hình để xem.
# ==============================================================================
def run_live(spark, args):
    valid, dead = parse(kafka_reader(spark, args, True))
    clean = dedup(valid, args.watermark)
    trigger = "5 seconds"

    def start(df, name, mode, fmt=None, writer=None, **options):
        w = df.writeStream.queryName(name).outputMode(mode).trigger(processingTime=trigger)
        w = w.option("checkpointLocation", f"{args.checkpoint_dir}/{name}")
        if writer is not None:
            w = w.foreachBatch(writer)
        else:
            w = w.format(fmt)
            for k, v in options.items():
                w = w.option(k, v)
        return w.start()

    start(clean, "transactions_pg", "append",
          writer=make_pg_writer(spark, args, "transactions", upsert_sql("transactions")))
    start(clean.withColumn("event_date", F.to_date("event_time")), "transactions_parquet", "append",
          fmt="parquet", path=f"{args.lake_dir}/transactions")
    start(daily_stats(clean), "daily_stats", "update",
          writer=make_pg_writer(spark, args, "daily_stats", upsert_sql("daily_stats")))
    start(customer_week(clean), "customer_week", "update",
          writer=make_pg_writer(spark, args, "customer_week", upsert_sql("customer_week")))
    start(dead, "dead_letter", "append",
          writer=make_pg_writer(spark, args, "dead_letter", DEAD_LETTER_UPSERT))
    if args.console:
        for name, df in (("console_daily_stats", daily_stats(clean)), ("console_customer_week", customer_week(clean))):
            start(df, name, "update", fmt="console", truncate="false", numRows=30)
    spark.streams.awaitAnyTermination()


# ==============================================================================
# Hàm: run_check
# - Mục đích: Chạy chế độ kiểm định (Verification mode `--check`) để đối chiếu streaming vs batch.
# - Điều cần chú ý:
#   + Sử dụng `availableNow=True` để xử lý dứt điểm toàn bộ dữ liệu đang tồn đọng trong Kafka vào Memory table.
#   + Đọc lại topic bằng Batch mode, tính toán độc lập và gọi `compare()`.
#   + Xuất báo cáo nghiệm thu JSON ra `data/reports/spark_check.json`.
# ==============================================================================
def run_check(spark, args):
    """Process everything currently in the topic, then compare with a batch recomputation."""
    valid, dead = parse(kafka_reader(spark, args, True))
    clean = dedup(valid, args.watermark)
    queries = []
    for name, df, mode in (("daily_stats", daily_stats(clean), "complete"),
                           ("customer_week", customer_week(clean), "complete"),
                           ("dead_letter", dead, "append")):
        queries.append(df.writeStream.queryName(name).outputMode(mode).format("memory")
                       .trigger(availableNow=True).start())
    for q in queries:
        q.awaitTermination()

    bvalid, bdead = parse(kafka_reader(spark, args, False))
    bclean = bvalid.dropDuplicates(["transaction_id", "event_time"])
    total_messages = spark.read.format("kafka").option("kafka.bootstrap.servers", args.bootstrap) \
        .option("subscribe", args.topic).option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest").load().count()
    distinct_tx = bclean.count()
    streamed_daily_total = spark.table("daily_stats").agg(F.sum("tx_count")).first()[0] or 0
    streamed_week_total = spark.table("customer_week").agg(F.sum("tx_count")).first()[0] or 0
    report = {
        "topic": args.topic,
        "kafka_messages": total_messages,
        "valid_distinct_transactions_batch": distinct_tx,
        "dead_letter_messages_stream": spark.table("dead_letter").count(),
        "daily_stats_total_tx_stream": int(streamed_daily_total),
        "customer_week_total_tx_stream": int(streamed_week_total),
        "daily_stats_vs_batch": compare(
            "daily_stats", spark.table("daily_stats"), daily_stats(bclean),
            ["window_start", "category"], ["tx_count", "total_amount", "avg_amount"]),
        "customer_week_vs_batch": compare(
            "customer_week", spark.table("customer_week"), customer_week(bclean),
            ["window_start", "customer_id"],
            ["tx_count", "total_amount", "avg_amount", "distinct_categories"]),
    }
    report["passed"] = (
        report["daily_stats_vs_batch"]["mismatched_rows"] == 0
        and report["customer_week_vs_batch"]["mismatched_rows"] == 0
        and report["daily_stats_total_tx_stream"] == distinct_tx
        and report["customer_week_total_tx_stream"] == distinct_tx
    )
    print(json.dumps(report, indent=2))
    spark.table("daily_stats").orderBy("window_start", "category").show(20, False)
    out = f"{args.reports_dir}/spark_check.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print("report written to", out)
    return 0 if report["passed"] else 1


# ==============================================================================
# Hàm: main
# - Mục đích: Điểm vào thực thi chính của ứng dụng Spark Structured Streaming.
# - Điều cần chú ý:
#   + Phân tích tham số dòng lệnh CLI (`--bootstrap`, `--topic`, `--watermark`, `--check`,...).
#   + Khởi tạo `SparkSession` với múi giờ UTC (`spark.sql.session.timeZone = "UTC"`),
#     và giới hạn shuffle partitions (`spark.sql.shuffle.partitions = "4"`).
#   + Rẽ nhánh thực thi chế độ `--check` (nghiệm thu) hoặc `run_live` (chạy thời gian thực).
# ==============================================================================
def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:19092", help="Kafka address as seen from the spark container")
    p.add_argument("--topic", default="transactions.raw")
    p.add_argument("--starting-offsets", default="earliest")
    p.add_argument("--watermark", default="1 day")
    p.add_argument("--checkpoint-dir", default="/workspace/data/checkpoints/live")
    p.add_argument("--lake-dir", default="/workspace/data/lake", help="Parquet data lake root")
    p.add_argument("--pg-url", default="jdbc:postgresql://postgres:5432/fraud")
    p.add_argument("--pg-user", default="fraud")
    p.add_argument("--pg-password", default="fraud")
    p.add_argument("--console", action="store_true", help="also print window results to the console")
    p.add_argument("--reports-dir", default="/workspace/data/reports")
    p.add_argument("--check", action="store_true", help="process the current topic, compare with batch, exit")
    args = p.parse_args(argv)

    spark = (SparkSession.builder.appName("fraud-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    if args.check:
        code = run_check(spark, args)
        spark.stop()
        sys.exit(code)
    run_live(spark, args)


if __name__ == "__main__":
    main()

