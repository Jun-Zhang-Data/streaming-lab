from pyspark.sql.functions import col, to_timestamp, when, lit
from src.common.delta_bootstrap import ensure_delta_table
from src.common.schemas import SILVER_SCHEMA, DLQ_SCHEMA

from src.common.spark import build_spark, append_progress_loop
from src.common.settings import (
    BRONZE_PATH, SILVER_PATH, DLQ_PATH,
    SILVER_CKPT, DLQ_CKPT, LOGS
)

def main():
    spark = build_spark("silver_clean_clicks")
    ensure_delta_table(spark, SILVER_PATH, SILVER_SCHEMA)
    ensure_delta_table(spark, DLQ_PATH, DLQ_SCHEMA)


    bronze = (
        spark.readStream
        .format("delta")
        .load(str(BRONZE_PATH))
    )

    with_time = bronze.withColumn("event_time", to_timestamp(col("event_ts")))

    is_valid = (
        col("event_id").isNotNull()
        & col("event_time").isNotNull()
        & col("country").isNotNull()
        & col("page").isNotNull()
        & col("user_id").isNotNull()
    )

    enriched = (
        with_time
        .withColumn("is_valid", is_valid)
        .withColumn(
            "error_reason",
            when(col("event_id").isNull(), lit("missing_event_id"))
            .when(col("event_time").isNull(), lit("bad_or_missing_event_ts"))
            .when(col("country").isNull(), lit("missing_country"))
            .when(col("page").isNull(), lit("missing_page"))
            .when(col("user_id").isNull(), lit("missing_user_id"))
            .otherwise(lit(None))
        )
    )

    dlq = enriched.filter(~col("is_valid"))
    valid = enriched.filter(col("is_valid")).drop("is_valid", "error_reason")

    deduped = (
        valid
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    q_dlq = (
        dlq.writeStream
        .format("delta")
        .option("path", str(DLQ_PATH))
        .option("checkpointLocation", str(DLQ_CKPT))
        .outputMode("append")
        .start()
    )

    q_silver = (
        deduped.writeStream
        .format("delta")
        .option("path", str(SILVER_PATH))
        .option("checkpointLocation", str(SILVER_CKPT))
        .outputMode("append")
        .start()
    )

    print("✅ SILVER started (Delta)")
    print("SILVER:", SILVER_PATH)
    print("DLQ   :", DLQ_PATH)

    try:
        append_progress_loop(q_silver, LOGS / "silver_progress.jsonl", every_seconds=10)
        append_progress_loop(q_dlq, LOGS / "dlq_progress.jsonl", every_seconds=10)
        q_silver.awaitTermination()
        q_dlq.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping SILVER/DLQ...")
    finally:
        q_silver.stop()
        q_dlq.stop()
        spark.stop()

if __name__ == "__main__":
    main()
