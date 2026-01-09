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

# Spark / Scala (match your working setup)
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

