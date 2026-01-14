from __future__ import annotations

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_timestamp,
    to_date,
    when,
)
from pyspark.sql.types import StringType

from src.common.settings import (
    DLQ_CLICKS_PATH,
    LOGS_DIR,
    PARTITION_COL,
    SILVER_CKPT,
    SILVER_CLICKS_PATH,
    SILVER_WATERMARK,
    TRIGGER_SECONDS,
)
from src.common.spark import build_spark, append_progress_loop


REQUIRED = ["event_id", "user_id", "page", "country", "event_ts"]


def main() -> None:
    spark = build_spark("silver-clean")

    bronze = (
        spark.readStream
        .format("delta")
        .load(str(__import__("src.common.settings", fromlist=["BRONZE_CLICKS_PATH"]).BRONZE_CLICKS_PATH))
    )

    # Parse event_time from event_ts (ISO string from producer)
    silver_base = (
        bronze
        .withColumn("event_time", to_timestamp(col("event_ts")))
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("silver_ingest_ts", current_timestamp())
    )

    # Build a simple error_reason (first failure found)
    error_reason = lit(None).cast(StringType())
    error_reason = when(col("event_id").isNull(), lit("missing_event_id")).otherwise(error_reason)
    error_reason = when(col("user_id").isNull(), lit("missing_user_id")).otherwise(error_reason)
    error_reason = when(col("page").isNull(), lit("missing_page")).otherwise(error_reason)
    error_reason = when(col("country").isNull(), lit("missing_country")).otherwise(error_reason)
    error_reason = when(col("event_time").isNull(), lit("bad_event_ts")).otherwise(error_reason)

    flagged = silver_base.withColumn("error_reason", error_reason)

    bad = flagged.where(col("error_reason").isNotNull())
    good = flagged.where(col("error_reason").isNull()).drop("error_reason")

    # DLQ write
    dlq_query = (
        bad.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(SILVER_CKPT) + "_dlq")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(str(DLQ_CLICKS_PATH))
    )

    # Dedup by event_id with watermark (stateful)
    deduped = (
        good
        .withWatermark("event_time", SILVER_WATERMARK)
        .dropDuplicates(["event_id"])
    )

    silver_query = (
        deduped.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(SILVER_CKPT))
        .partitionBy(PARTITION_COL)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(str(SILVER_CLICKS_PATH))
    )

    try:
        append_progress_loop(silver_query, LOGS_DIR / "silver_progress.jsonl", every_seconds=10)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        dlq_query.stop()
        silver_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

