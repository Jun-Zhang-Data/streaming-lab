from __future__ import annotations

from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, window

from src.common.audit import write_audit
from src.common.settings import (
    AUDIT_PATH,
    GOLD_CKPT,
    GOLD_COUNTRY_MINUTE_PATH,
    GOLD_WATERMARK,
    LOGS_DIR,
    SILVER_CLICKS_PATH,
    TRIGGER_SECONDS,
)
from src.common.spark import build_spark, append_progress_loop


def upsert_gold_batch(spark, batch_df: DataFrame, batch_id: int) -> None:
    """
    MERGE this micro-batch of aggregates into the Gold Delta table.
    Key: (window_start, window_end, country)
    """
    if batch_df is None:
        return
    if batch_df.rdd.isEmpty():
        # Optional audit
        try:
            write_audit(spark, str(AUDIT_PATH), "gold_country_minute", batch_id, 0, 0, note="empty_batch")
        except Exception:
            pass
        return

    gold_path = str(GOLD_COUNTRY_MINUTE_PATH)

    # Create table on first write
    if not DeltaTable.isDeltaTable(spark, gold_path):
        (batch_df.write
         .format("delta")
         .mode("overwrite")
         .save(gold_path))
        try:
            write_audit(spark, str(AUDIT_PATH), "gold_country_minute", batch_id,
                        rows_in=batch_df.count(), rows_out=batch_df.count(), note="create_table_overwrite")
        except Exception:
            pass
        return

    tgt = DeltaTable.forPath(spark, gold_path)

    (
        tgt.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.window_start = s.window_start AND "
            "t.window_end = s.window_end AND "
            "t.country = s.country"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    try:
        n = batch_df.count()
        write_audit(spark, str(AUDIT_PATH), "gold_country_minute", batch_id, n, n, note="merge_upsert")
    except Exception:
        pass


def main() -> None:
    spark = build_spark("gold-agg")

    silver = (
        spark.readStream
        .format("delta")
        .load(str(SILVER_CLICKS_PATH))
    )

    agg = (
        silver
        .withWatermark("event_time", GOLD_WATERMARK)
        .groupBy(window(col("event_time"), "1 minute"), col("country"))
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("country"),
            col("count").alias("events"),
        )
        .withColumn("event_date", to_date(col("window_start")))
    )

    query = (
        agg.writeStream
        .foreachBatch(lambda df, bid: upsert_gold_batch(spark, df, bid))
        .option("checkpointLocation", str(GOLD_CKPT))
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    try:
        append_progress_loop(query, LOGS_DIR / "gold_progress.jsonl", every_seconds=10)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

