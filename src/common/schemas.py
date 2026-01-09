from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, TimestampType, BooleanType
)

CLICK_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_ts", StringType(), True),
])

# Silver output (after adding event_time)
SILVER_SCHEMA = StructType([
    StructField("raw_json", StringType(), True),
    StructField("kafka_key", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("ingest_ts", TimestampType(), True),

    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("event_time", TimestampType(), True),
])

# DLQ output (same as silver + is_valid/error_reason)
DLQ_SCHEMA = StructType(list(SILVER_SCHEMA.fields) + [
    StructField("is_valid", BooleanType(), True),
    StructField("error_reason", StringType(), True),
])
