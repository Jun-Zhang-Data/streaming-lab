from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import Row, SparkSession


def write_audit(
    spark: SparkSession,
    audit_path: str,
    job: str,
    batch_id: int,
    rows_in: int,
    rows_out: int,
    note: str = "",
    extra: Optional[str] = None,
) -> None:
    """
    Appends a single-row audit entry into a Delta table (JSON-like logs you can query).
    """
    now = datetime.now(timezone.utc).isoformat()
    row = Row(
        ts_utc=now,
        job=job,
        batch_id=int(batch_id),
        rows_in=int(rows_in),
        rows_out=int(rows_out),
        note=note,
        extra=extra or "",
    )
    df = spark.createDataFrame([row])
    df.write.format("delta").mode("append").save(audit_path)
