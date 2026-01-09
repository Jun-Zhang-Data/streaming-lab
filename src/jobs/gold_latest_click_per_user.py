from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from src.common.spark import build_spark, append_progress_loop
from src.common.settings import SILVER_PATH, LOGS, OUT, CKPT

LATEST_PATH = OUT / "gold" / "latest_click_per_user"
LATEST_CKPT = CKPT / "gold" / "latest_click_per_user"


def upsert_latest(microbatch_df, batch_id: int):
    # pick latest per user within this micro-batch
    w = Window.partitionBy("user_id").orderBy(col("event_time").desc())
    latest = (
        microbatch_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .select("user_id", "event_id", "event_time", "country", "page", "event_ts")
    )

    if DeltaTable.isDeltaTable(microbatch_df.sparkSession, str(LATEST_PATH)):
        tgt = DeltaTable.forPath(microbatch_df.sparkSession, str(LATEST_PATH))
        (
            tgt.alias("t")
            .merge(latest.alias("s"), "t.user_id = s.user_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        latest.write.format("delta").mode("overwrite").save(str(LATEST_PATH))


def main():
    spark = build_spark("gold_latest_click_per_user")

    silver = (
        spark.readStream
        .format("delta")
        .load(str(SILVER_PATH))
    )

    q = (
        silver.writeStream
        .foreachBatch(upsert_latest)
        .option("checkpointLocation", str(LATEST_CKPT))
        .start()
    )

    print("✅ GOLD latest_click_per_user started")
    print("Path:", LATEST_PATH)

    try:
        append_progress_loop(q, LOGS / "gold_latest_progress.jsonl", every_seconds=10)
        q.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
