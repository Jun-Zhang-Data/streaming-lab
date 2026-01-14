from __future__ import annotations

from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

from src.common.settings import (
    BRONZE_CKPT,
    BRONZE_CLICKS_PATH,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC_CLICKS,
    LOGS_DIR,
    MAX_OFFSETS_PER_TRIGGER,
    STARTING_OFFSETS,
    TRIGGER_SECONDS,
)
from src.common.spark import build_spark, append_progress_loop


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("page", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_ts", StringType(), True),
])


def main() -> None:
    spark = build_spark("bronze-ingest")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_CLICKS)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka 'value' is bytes; cast to string = raw JSON
    raw = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
    ).withColumn("ingest_ts", current_timestamp())

    parsed = raw.withColumn("j", from_json(col("raw_json"), EVENT_SCHEMA)).select(
        "*",
        col("j.event_id").alias("event_id"),
        col("j.user_id").alias("user_id"),
        col("j.page").alias("page"),
        col("j.country").alias("country"),
        col("j.event_ts").alias("event_ts"),
    ).drop("j")

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(BRONZE_CKPT))
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(str(BRONZE_CLICKS_PATH))
    )

    # progress log
    try:
        append_progress_loop(query, LOGS_DIR / "bronze_progress.jsonl", every_seconds=10)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

