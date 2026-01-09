# src/common/delta_bootstrap.py
from __future__ import annotations

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from delta.tables import DeltaTable


def ensure_delta_table(spark: SparkSession, path: Path, schema: StructType) -> None:
    """
    Ensure a Delta table exists at `path` with a committed schema.
    If the folder exists but schema isn't committed yet, we bootstrap with empty write.
    """
    path_str = str(path)

    try:
        # If this works, schema is already committed
        if DeltaTable.isDeltaTable(spark, path_str):
            _ = spark.read.format("delta").load(path_str).schema
            return
    except Exception:
        # schema not set / broken delta log -> bootstrap below
        pass

    empty_df: DataFrame = spark.createDataFrame([], schema)
    (
        empty_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path_str)
    )
