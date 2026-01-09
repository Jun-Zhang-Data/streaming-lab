# src/jobs/gold_agg.py
from delta.tables import DeltaTable
from pyspark.sql.functions import col, window

from src.common.spark import build_spark, append_progress_loop
from src.common.settings import (
    SILVER_PATH,
    GOLD_COUNTRY_MINUTE_PATH,
    GOLD_COUNTRY_MINUTE_CKPT,
    LOGS,
)

KEY_COND = """
t.window_start = s.window_start AND
t.window_end   = s.window_end   AND
t.country      = s.country
"""


def upsert_kpi(microbatch_df, batch_id: int):
    """
    microbatch_df contains updates for aggregations (because we run outputMode('update')).
    We upsert into a Delta table so the table always reflects the latest counts.
    """
    spark = microbatch_df.sparkSession
    path = str(GOLD_COUNTRY_MINUTE_PATH)

    # If batch is empty, do nothing
    if microbatch_df.rdd.isEmpty():
        return

    # Create table if missing
    if not DeltaTable.isDeltaTable(spark, path):
        (microbatch_df
         .write
         .format("delta")
         .mode("overwrite")
         .save(path))
        return

    tgt = DeltaTable.forPath(spark, path)

    (tgt.alias("t")
        .merge(microbatch_df.alias("s"), KEY_COND)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def main():
    spark = build_spark("gold_agg_clicks")

    silver = (
        spark.readStream
        .format("delta")
        .load(str(SILVER_PATH))
    )

    agg = (
        silver
        # Make results appear sooner (still handles late data)
        .withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), "1 minute"), col("country"))
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("country"),
            col("count").alias("events"),
        )
    )

    q = (
        agg.writeStream
        .foreachBatch(upsert_kpi)
        .option("checkpointLocation", str(GOLD_COUNTRY_MINUTE_CKPT))
        .outputMode("update")  # allowed with foreachBatch
        .start()
    )

    print("GOLD started (Delta via foreachBatch MERGE)")

    try:
        append_progress_loop(q, LOGS / "gold_progress.jsonl", every_seconds=10)
        q.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping GOLD...")
    finally:
        q.stop()
        spark.stop()


if __name__ == "__main__":
    main()

