"""Check the Parquet data lake written by the live job. Run inside the spark container:
    docker compose run --rm spark /workspace/scripts/check_parquet.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("check-parquet").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("/workspace/data/lake/transactions")  # only committed files (_spark_metadata)
print("rows              :", df.count())
print("distinct tx ids   :", df.select("transaction_id").distinct().count())
print("partition columns :", "event_date" in df.columns)
print("days (event_date) :", df.select("event_date").distinct().count())
df.groupBy("event_date").count().orderBy("event_date").show(10, False)
spark.stop()

