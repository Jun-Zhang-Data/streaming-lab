# spark_stream.py
# Reads click events from Kafka, parses JSON, aggregates by event-time window,
# writes Parquet output + checkpoint, and prints streaming progress so you can
# verify offsets/checkpointing are working.

import os
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- Paths (absolute, Windows-friendly) ---
BASE = os.path.abspath(os.path.dirname(__file__))
OUT_PATH = os.path.join(BASE, "out", "agg_parquet")
CKPT_PATH = os.path.join(BASE, "out", "_checkpoints", "agg_parquet")

# --- Spark session ---
# IMPORTANT: Spark 3.5.5 on Windows typically uses Scala 2.12,
# so the Kafka connector artifacts must be *_2.12 and match Spark version.
spark = (
    SparkSession.builder
    .appName("clicks-stream")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5",
        ])
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --- Event schema ---
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_ts", StringType(), True),  # ISO timestamp string
])

# --- Read from Kafka (streaming source) ---
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "clicks")
    # If you want to replay from earliest, delete checkpoint folder first.
    .option("startingOffsets", "latest")
    .load()
)

# --- Parse JSON payload and derive event time ---
parsed = (
    kafka_df.selectExpr("CAST(value AS STRING) AS value_str")
    .select(from_json(col("value_str"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_time", to_timestamp(col("event_ts")))
)

# --- Windowed aggregation (event-time) ---
agg = (
    parsed
    .withWatermark("event_time", "2 minutes")               # allow late events
    .groupBy(window(col("event_time"), "1 minute"), col("country"))
    .count()
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("country"),
        col("count").alias("events"),
    )
)

# --- Write streaming output + checkpoint ---
query = (
    agg.writeStream
    .format("parquet")
    .option("path", OUT_PATH)
    .option("checkpointLocation", CKPT_PATH)
    .outputMode("append")
    .start()
)

# --- Print IDs (so you can tell restarts apart) ---
print("✅ Streaming started")
print("Output path:", OUT_PATH)
print("Checkpoint:", CKPT_PATH)
print("Query id:", query.id)
print("Run id:", query.runId)
print("Tip: Stop with Ctrl+C. Restart with same checkpoint to verify it resumes.")

# --- Live progress (shows Kafka offsets) ---
try:
    while query.isActive:
        time.sleep(5)
        lp = query.lastProgress
        if lp:
            print("----- lastProgress -----")
            print(json.dumps(lp, indent=2))
        else:
            print("(no progress yet — waiting for first micro-batch)")
except KeyboardInterrupt:
    print("\nStopping...")
finally:
    query.stop()
    spark.stop()

