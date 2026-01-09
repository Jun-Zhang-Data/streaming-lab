from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("kafka-smoketest")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5",
        ])
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "clicks")
    .option("startingOffsets", "earliest")
    .load()
)

out = df.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value",
    "topic", "partition", "offset"
)

q = (
    out.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start()
)

print("✅ Streaming started. You should see events printing below...")
q.awaitTermination()

