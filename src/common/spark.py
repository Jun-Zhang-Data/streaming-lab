from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Spark 3.5.5 uses Scala 2.12 -> use _2.12 artifacts
SPARK_KAFKA_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5"
)


def build_spark(app_name: str) -> SparkSession:
    """
    SparkSession with:
      - Delta enabled
      - Kafka connector jars
      - Explicit Python executable (important on Windows; avoids 'python3' lookup)
    """
    extra_packages: List[str] = [p.strip() for p in SPARK_KAFKA_PACKAGES.split(",") if p.strip()]
    python_exe = sys.executable  # conda env python.exe

    # Ensure env vars exist for child JVM/worker processes
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Force Python executable for workers + driver
        .config("spark.pyspark.python", python_exe)
        .config("spark.pyspark.driver.python", python_exe)
        # Delta extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Windows cleanup helpfulness (still may see temp/snappy warnings)
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # Optional: auto-pick a free Spark UI port
        .config("spark.ui.port", "0")
        # Ensure Kafka packages are present
        .config("spark.jars.packages", ",".join(extra_packages))
    )

    # IMPORTANT: Delta helper can overwrite jar configs; pass Kafka packages here too
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def append_progress_loop(query, log_path: Path, every_seconds: int = 10) -> None:
    """
    Writes one JSON per micro-batch into a .jsonl log file.
    Good for debugging offsets/rows/batch timings without spamming console.
    """
    ensure_dir(log_path.parent)
    last_batch_id: Optional[int] = None

    while query.isActive:
        time.sleep(every_seconds)
        lp = query.lastProgress
        if not lp:
            continue
        batch_id = lp.get("batchId")
        if batch_id is not None and batch_id == last_batch_id:
            continue
        last_batch_id = batch_id
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(lp) + "\n")


