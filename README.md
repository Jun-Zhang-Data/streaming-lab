# Streaming Lab (Kafka/Redpanda + Spark Structured Streaming) — Bronze/Silver/Gold

This repo is a local “real work style” streaming project on Windows:
- Kafka-compatible broker: Redpanda (Docker)
- Stream processing: Spark Structured Streaming (PySpark 3.5.5)
- Outputs: Parquet files with checkpoints (restart/resume)
- Layers: Bronze → Silver → Gold + DLQ + progress logs

## Prereqs
- Docker Desktop
- Conda
- Spark 3.5.5 installed (you currently have it at):
  C:\Program Files\spark-3.5.5-bin-hadoop3

## One-time Windows Spark fix
You already did this, but for reference Spark on Windows needs:
- C:\hadoop\bin\winutils.exe
- C:\hadoop\bin\hadoop.dll
and env:
- HADOOP_HOME=C:\hadoop
- PATH contains C:\hadoop\bin

You can set these per terminal using scripts\set_env.ps1.

## Setup
1) Create/activate conda env
```powershell
conda create -n streaming_lab python=3.11 -y
conda activate streaming_lab
pip install -r requirements.txt

------------------------------------------------------------------------------------------------------------------------------
Create the whole repo (copy/paste as-is)
# ====== CREATE FULL REPO STRUCTURE (run from repo root) ======
$ErrorActionPreference = "Stop"

# Safety check: confirm we're in the right folder (must contain docker-compose.yml)
if (!(Test-Path ".\docker-compose.yml")) {
  throw "Run this from the repo root folder that contains docker-compose.yml"
}

# Folders
mkdir .\scripts -Force | Out-Null
mkdir .\src\common -Force | Out-Null
mkdir .\src\jobs -Force | Out-Null
mkdir .\data\reference -Force | Out-Null
mkdir .\logs -Force | Out-Null
mkdir .\out -Force | Out-Null

# Make src a package
"" | Set-Content -Encoding UTF8 .\src\__init__.py
"" | Set-Content -Encoding UTF8 .\src\common\__init__.py
"" | Set-Content -Encoding UTF8 .\src\jobs\__init__.py

# ---- requirements.txt (keep your existing if present) ----
if (!(Test-Path .\requirements.txt)) {
@'
confluent-kafka>=2.3.0
pyspark==3.5.5
'@ | Set-Content -Encoding UTF8 .\requirements.txt
}

# ---- src/common/settings.py ----
@'
from __future__ import annotations
import os
from pathlib import Path

def project_root() -> Path:
    return Path(__file__).resolve().parents[2]

ROOT = project_root()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_CLICKS = os.environ.get("KAFKA_TOPIC_CLICKS", "clicks")
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "latest")  # latest|earliest|json

# Spark/Scala (match your working setup)
SPARK_VERSION = os.environ.get("SPARK_VERSION", "3.5.5")
SCALA_BINARY_VERSION = os.environ.get("SCALA_BINARY_VERSION", "2.12")

SPARK_KAFKA_PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY_VERSION}:{SPARK_VERSION}",
    f"org.apache.spark:spark-token-provider-kafka-0-10_{SCALA_BINARY_VERSION}:{SPARK_VERSION}",
])

# Output locations
OUT = ROOT / "out"
LOGS = ROOT / "logs"

BRONZE_PATH = OUT / "bronze" / "clicks_parquet"
SILVER_PATH = OUT / "silver" / "clicks_clean"
GOLD_COUNTRY_MINUTE_PATH = OUT / "gold" / "kpi_country_minute"
DLQ_PATH = OUT / "dlq" / "clicks_dlq"

CKPT = OUT / "_checkpoints"
BRONZE_CKPT = CKPT / "bronze" / "clicks_parquet"
SILVER_CKPT = CKPT / "silver" / "clicks_clean"
GOLD_COUNTRY_MINUTE_CKPT = CKPT / "gold" / "kpi_country_minute"
DLQ_CKPT = CKPT / "dlq" / "clicks_dlq"
'@ | Set-Content -Encoding UTF8 .\src\common\settings.py

# ---- src/common/schemas.py ----
@'
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

CLICK_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_ts", StringType(), True),
])
'@ | Set-Content -Encoding UTF8 .\src\common\schemas.py

# ---- src/common/spark.py ----
@'
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession
from .settings import SPARK_KAFKA_PACKAGES

def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGES)
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def append_progress_loop(query, log_path: Path, every_seconds: int = 10) -> None:
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
'@ | Set-Content -Encoding UTF8 .\src\common\spark.py

# ---- src/producer.py (copy your existing producer.py content) ----
Copy-Item .\producer.py .\src\producer.py -Force

# ---- src/smoketest_kafka_console.py (copy your working smoketest) ----
Copy-Item .\spark_kafka_smoketest.py .\src\smoketest_kafka_console.py -Force

# ---- src/jobs/bronze_ingest.py ----
@'
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

    query = (
        bronze.writeStream
        .format("parquet")
        .option("path", str(BRONZE_PATH))
        .option("checkpointLocation", str(BRONZE_CKPT))
        .outputMode("append")
        .start()
    )

    print("✅ BRONZE started")
    print("BRONZE path:", BRONZE_PATH)
    print("Checkpoint :", BRONZE_CKPT)

    try:
        append_progress_loop(query, LOGS / "bronze_progress.jsonl", every_seconds=10)
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping BRONZE...")
    finally:
        query.stop()
        spark.stop()

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 .\src\jobs\bronze_ingest.py

# ---- src/jobs/silver_clean.py ----
@'
from pyspark.sql.functions import col, to_timestamp, when, lit

from src.common.spark import build_spark, append_progress_loop
from src.common.settings import (
    BRONZE_PATH, SILVER_PATH, DLQ_PATH,
    SILVER_CKPT, DLQ_CKPT, LOGS
)

def main():
    spark = build_spark("silver_clean_clicks")

    bronze = (
        spark.readStream
        .format("parquet")
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
        .format("parquet")
        .option("path", str(DLQ_PATH))
        .option("checkpointLocation", str(DLQ_CKPT))
        .outputMode("append")
        .start()
    )

    q_silver = (
        deduped.writeStream
        .format("parquet")
        .option("path", str(SILVER_PATH))
        .option("checkpointLocation", str(SILVER_CKPT))
        .outputMode("append")
        .start()
    )

    print("✅ SILVER started")
    print("SILVER path:", SILVER_PATH)
    print("DLQ path   :", DLQ_PATH)

    try:
        # progress logging (simple)
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
'@ | Set-Content -Encoding UTF8 .\src\jobs\silver_clean.py

# ---- src/jobs/gold_agg.py ----
@'
from pyspark.sql.functions import col, window

from src.common.spark import build_spark, append_progress_loop
from src.common.settings import (
    SILVER_PATH, GOLD_COUNTRY_MINUTE_PATH,
    GOLD_COUNTRY_MINUTE_CKPT, LOGS
)

def main():
    spark = build_spark("gold_agg_clicks")

    silver = (
        spark.readStream
        .format("parquet")
        .load(str(SILVER_PATH))
    )

    agg = (
        silver
        .withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("country"))
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("country"),
            col("count").alias("events")
        )
    )

    q_gold = (
        agg.writeStream
        .format("parquet")
        .option("path", str(GOLD_COUNTRY_MINUTE_PATH))
        .option("checkpointLocation", str(GOLD_COUNTRY_MINUTE_CKPT))
        .outputMode("append")
        .start()
    )

    print("✅ GOLD started")
    print("GOLD path :", GOLD_COUNTRY_MINUTE_PATH)

    try:
        append_progress_loop(q_gold, LOGS / "gold_progress.jsonl", every_seconds=10)
        q_gold.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping GOLD...")
    finally:
        q_gold.stop()
        spark.stop()

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 .\src\jobs\gold_agg.py

# ---- data/reference/country_dim.csv ----
@'
country,country_name
SE,Sweden
NO,Norway
DK,Denmark
FI,Finland
DE,Germany
NL,Netherlands
'@ | Set-Content -Encoding UTF8 .\data\reference\country_dim.csv

# ---- scripts/set_env.ps1 ----
@'
$ErrorActionPreference = "Stop"
$env:HADOOP_HOME="C:\hadoop"
$env:hadoop_home_dir="C:\hadoop"
$env:PATH="C:\hadoop\bin;$env:PATH"
Write-Host "HADOOP_HOME=$env:HADOOP_HOME"
'@ | Set-Content -Encoding UTF8 .\scripts\set_env.ps1

# ---- scripts/up.ps1 ----
@'
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

docker compose up -d
Start-Sleep -Seconds 2

$container = docker ps -q --filter "name=redpanda"
if ($container) {
  # create topic (ignore errors if exists)
  docker exec -it $container rpk topic create clicks -p 3
  docker exec -it $container rpk topic describe clicks
}

Write-Host "Kafka UI: http://localhost:8081"
'@ | Set-Content -Encoding UTF8 .\scripts\up.ps1

# ---- scripts/down.ps1 ----
@'
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
docker compose down
'@ | Set-Content -Encoding UTF8 .\scripts\down.ps1

# ---- scripts/run_producer.ps1 ----
@'
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
conda activate streaming_lab | Out-Null
python -m src.producer
'@ | Set-Content -Encoding UTF8 .\scripts\run_producer.ps1

# ---- scripts/run_smoketest.ps1 ----
@'
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
. "$PSScriptRoot\set_env.ps1"
conda activate streaming_lab | Out-Null
python -m src.smoketest_kafka_console
'@ | Set-Content -Encoding UTF8 .\scripts\run_smoketest.ps1

# ---- scripts/run_bronze.ps1 ----
@'
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
. "$PSScriptRoot\set_env.ps1"
conda activate streaming_lab | Out-Null
python -m src.jobs.bronze_ingest
'@ | Set-Content -Encoding UTF8 .\scripts\run_bronze.ps1

# ---- scripts/run_silver.ps1 ----
@'
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
. "$PSScriptRoot\set_env.ps1"
conda activate streaming_lab | Out-Null
python -m src.jobs.silver_clean
'@ | Set-Content -Encoding UTF8 .\scripts\run_silver.ps1

# ---- scripts/run_gold.ps1 ----
@'
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
. "$PSScriptRoot\set_env.ps1"
conda activate streaming_lab | Out-Null
python -m src.jobs.gold_agg
'@ | Set-Content -Encoding UTF8 .\scripts\run_gold.ps1

Write-Host "✅ Repo structure created. Try: dir .\scripts"
# ====== END ======
-----------------------------------------------------------------------------------------------------------------------------




1) What this project is (the big picture)

You built a local streaming lakehouse:

Producer → Kafka (Redpanda) → Spark streaming → Delta Lake (Bronze/Silver/Gold + DLQ)

Producer: generates click events continuously (JSON)

Kafka / Redpanda: holds events in a topic (clicks)

Spark Structured Streaming: reads events continuously and transforms them

Delta Lake (ACID tables on disk): stores outputs in transaction-safe tables

Bronze/Silver/Gold: “layers” that mirror real company pipelines

DLQ: “dead letter queue” table where bad records go

You can run it without cloud. Everything is local.

2) The data that flows through the system

Each event looks like this (JSON):

{
  "event_id": "uuid",
  "user_id": 64,
  "page": "/docs",
  "country": "FI",
  "event_ts": "2026-01-09T12:37:29.749115+00:00"
}


Important fields:

event_id: unique id (used for dedupe)

event_ts: event time from the source (used for event-time windows)

user_id, page, country: dimensions for analytics

3) Repo layout (what each folder is for)
Root

docker-compose.yml — starts Redpanda + Kafka UI

requirements.txt — Python deps (pyspark + delta-spark + confluent-kafka)

README.md — runbook

src/

All Python code.

src/producer.py

Produces events into Kafka topic clicks

src/common/

Shared utilities:

settings.py

defines constants like Kafka bootstrap server and output paths:

out/bronze/..., out/silver/..., out/gold/...

checkpoints under out/_checkpoints/...

schemas.py

defines expected JSON schema for parsing click events

spark.py

creates Spark session correctly for Windows + Delta + Kafka

includes fixes you needed:

Delta enabled

Kafka connector jars added

Python executable forced (avoid “python3 not found”)

append_progress_loop() logs streaming progress to logs/

src/jobs/

Streaming jobs:

bronze_ingest.py

silver_clean.py

gold_agg.py

out/

Where Delta tables + checkpoints live (DO NOT commit to GitHub)

logs/

Runtime logs + progress logs (also do not commit)

scripts/

PowerShell helpers (up, down, run_*, start_all, stop_all)

4) What each pipeline stage does
4.1 Bronze — src/jobs/bronze_ingest.py

Purpose: store raw ingestion “as received” (audit layer)

Reads from Kafka topic clicks and writes to Delta table:

out/bronze/clicks_parquet (name kept from earlier, but it’s Delta)

It stores:

raw_json (original event string)

Kafka metadata: topic, partition, offset, kafka_timestamp

ingest_ts (processing time when Bronze saw it)

parsed fields (best-effort): event_id, user_id, page, country, event_ts

Why Bronze matters in real work:

you can replay / rebuild downstream

you can audit exactly what came from source + offsets

4.2 Silver — src/jobs/silver_clean.py

Purpose: clean, validate, dedupe, standardize

Reads Bronze Delta and:

parses event_ts into event_time (Timestamp)

validates required fields:

event_id, event_time, country, page, user_id

splits data:

valid rows → Silver

invalid rows → DLQ

dedupes valid events using:

withWatermark("event_time", "10 minutes")

dropDuplicates(["event_id"])

Writes:

Silver: out/silver/clicks_clean (Delta)

DLQ: out/dlq/clicks_dlq (Delta)

Why dedupe + watermark matters:

streaming pipelines can see duplicates (retries/replays)

watermark limits how long Spark keeps state

4.3 Gold — src/jobs/gold_agg.py

Purpose: business KPIs

Reads Silver and computes:

events per country per 1-minute window

Key output columns:

window_start, window_end, country, events

Writes to:

out/gold/kpi_country_minute (Delta)

Important detail: Delta streaming sink doesn’t support outputMode("update") directly.
So Gold uses foreachBatch + MERGE:

each microbatch produces updated counts

MERGE upserts into Delta table keyed by (window_start, window_end, country)

result: the table always reflects latest counts, like a dashboard table

5) Checkpoints (why you have _checkpoints)

Each job has its own checkpoint folder under:

out/_checkpoints/<layer>/<table>

This stores:

which offsets/files were processed

state for dedupe / windowing

Why checkpoints matter:

if you stop and restart a job, it continues from where it left off

deleting checkpoints forces a “reprocess”

6) How to run it
Option A: multiple terminals (simplest conceptually)

.\scripts\up.ps1

.\scripts\run_producer.ps1

.\scripts\run_bronze.ps1

.\scripts\run_silver.ps1

.\scripts\run_gold.ps1

Stop each with Ctrl+C.

Option B: one terminal (recommended for you)

Start all:

.\scripts\start_all.ps1


Stop all:

.\scripts\stop_all.ps1


Logs:

logs/gold.out.log, logs/gold.err.log, etc.

7) How to verify it’s working (most important)
7.1 Delta tables exist

A Delta table must have _delta_log:

Test-Path .\out\bronze\clicks_parquet\_delta_log
Test-Path .\out\silver\clicks_clean\_delta_log
Test-Path .\out\gold\kpi_country_minute\_delta_log
Test-Path .\out\dlq\clicks_dlq\_delta_log

7.2 Query results (Gold KPI)
python -c "from src.common.spark import build_spark; from src.common.settings import GOLD_COUNTRY_MINUTE_PATH; s=build_spark('peek-gold'); df=s.read.format('delta').load(str(GOLD_COUNTRY_MINUTE_PATH)); print('count=', df.count()); df.orderBy('window_start', ascending=False).show(50, truncate=False); s.stop()"


If count > 0, you’re good.

7.3 Check DLQ
python -c "from src.common.spark import build_spark; from src.common.settings import DLQ_PATH; s=build_spark('peek-dlq'); df=s.read.format('delta').load(str(DLQ_PATH)); print('count=', df.count()); df.orderBy('ingest_ts', ascending=False).show(20, truncate=False); s.stop()"

8) Common “noise” you can ignore

Spark UI port warnings (4040/4041/4042)

“Failed to delete Spark temp dir / snappy jar” (Windows file locking)

They do not mean the pipeline is wrong.

9) What a real data engineer would do next (your roadmap)

Runbook & replay

scripts to rebuild Silver+Gold from Bronze

scripts to replay from Kafka earliest (wipe Bronze checkpoint)

Data quality metrics

a gold table: invalid_rate per minute, total_events per minute

Schema contract

store JSON schema/Avro for events

(optional) register in Schema Registry

More realistic source

a second topic (purchases) + join → “conversion rate” KPI
