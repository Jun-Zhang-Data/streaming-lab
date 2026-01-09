from pyspark.sql.functions import col, from_json, current_timestamp

from src.common.spark import build_spark, append_progress_loop
from src.common.schemas import CLICK_EVENT_SCHEMA
from src.common.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_CLICKS, KAFKA_STARTING_OFFSETS,
    BRONZE_PATH, BRONZE_CKPT, LOGS
)

def main():
    spark = build_spark("bronze_ingest_clicks")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_CLICKS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .load()
    )

    bronze = (
        kafka_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_json",
            "topic", "partition", "offset", "timestamp AS kafka_timestamp"
        )
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("e", from_json(col("raw_json"), CLICK_EVENT_SCHEMA))
        .select(
            "raw_json",
            "kafka_key",
            "topic", "partition", "offset", "kafka_timestamp",
            "ingest_ts",
            col("e.event_id").alias("event_id"),
            col("e.user_id").alias("user_id"),
            col("e.page").alias("page"),
            col("e.country").alias("country"),
            col("e.event_ts").alias("event_ts"),
        )
    )

    q = (
        bronze.writeStream
        .format("delta")
        .option("path", str(BRONZE_PATH))
        .option("checkpointLocation", str(BRONZE_CKPT))
        .outputMode("append")
        .start()
    )

    print("✅ BRONZE started (Delta)")
    print("Path:", BRONZE_PATH)

    try:
        append_progress_loop(q, LOGS / "bronze_progress.jsonl", every_seconds=10)
        q.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping BRONZE...")
    finally:
        q.stop()
        spark.stop()

if __name__ == "__main__":
    main()
