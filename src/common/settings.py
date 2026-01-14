from __future__ import annotations

from pathlib import Path

# Repo root (src/common/settings.py -> parents[2] == repo root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUT_DIR = PROJECT_ROOT / "out"
LOGS_DIR = PROJECT_ROOT / "logs"

# --- Kafka ---
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC_CLICKS = "clicks"

# --- Delta table paths ---
BRONZE_CLICKS_PATH = OUT_DIR / "bronze" / "clicks_parquet"
SILVER_CLICKS_PATH = OUT_DIR / "silver" / "clicks_clean"
DLQ_CLICKS_PATH = OUT_DIR / "dlq" / "clicks_dlq"
GOLD_COUNTRY_MINUTE_PATH = OUT_DIR / "gold" / "kpi_country_minute"

# --- Checkpoints ---
CHK_DIR = OUT_DIR / "_checkpoints"
BRONZE_CKPT = CHK_DIR / "bronze_clicks"
SILVER_CKPT = CHK_DIR / "silver_clicks"
GOLD_CKPT = CHK_DIR / "gold_country_minute"

# --- Audit (optional but very useful) ---
AUDIT_PATH = OUT_DIR / "audit" / "stream_audit"

# --- Streaming knobs (safe beginner defaults) ---
TRIGGER_SECONDS = 10                 # micro-batch interval
MAX_OFFSETS_PER_TRIGGER = 5000       # rate limit from Kafka
STARTING_OFFSETS = "latest"          # "earliest" for backfill/testing

# --- Watermarks (tune later) ---
SILVER_WATERMARK = "10 minutes"      # dedup state window
GOLD_WATERMARK = "10 minutes"        # aggregation lateness window

# Partitioning column name
PARTITION_COL = "event_date"
