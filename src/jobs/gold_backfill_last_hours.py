from __future__ import annotations

import sys
from delta.tables import DeltaTable
from pyspark.sql.functions import col, expr, to_date, window

from src.common.settings import GOLD_COUNTRY_MINUTE_PATH, SILVER_CLICKS_PATH
from src.common.spark import build_spark


def upsert_gold(spark, df):
    gold_path = str(GOLD_COUNTRY_MINUTE_PATH)

    if df.rdd.isEmpty():
        print("No rows to upsert.")
        return

    if not DeltaTable.isDeltaTable(spark, gold_path):
        df.write.format("delta").mode("overwrite").save(gold_path)
        print("Gold table created.")
        return

    tgt = DeltaTable.forPath(spark, gold_path)
    (
        tgt.alias("t")
        .merge(
            df.alias("s"),
            "t.window_start = s.window_start AND t.window_end = s.window_end AND t.country = s.country"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Gold MERGE completed.")


def main() -> None:
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    spark = build_spark(f"gold-backfill-{hours}h")

    silver = spark.read.format("delta").load(str(SILVER_CLICKS_PATH))
    cutoff = expr(f"current_timestamp() - INTERVAL {hours} HOURS")

    recent = silver.where(col("event_time") >= cutoff)

    agg = (
        recent
        .groupBy(window(col("event_time"), "1 minute"), col("country"))
        .count()
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "country",
            "count as events"
        )
        .withColumn("event_date", to_date(col("window_start")))
    )

    upsert_gold(spark, agg)
    spark.stop()


if __name__ == "__main__":
    main()
