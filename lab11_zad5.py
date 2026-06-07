from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from pyspark.sql.functions import (
    col,
    to_timestamp,
    count,
    sum
)

spark = SparkSession.builder \
    .appName("LAB11_Checkpointing") \
    .getOrCreate()

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])

df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("data/input_stream")

df = df.withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

df = df.filter(
    col("status") == "paid"
)

summary = df.groupBy("category").agg(
    count("*").alias("events_count"),
    sum("amount").alias("total_amount")
)

console_query = summary.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

file_query = df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "data/output_stream"
    ) \
    .option(
        "checkpointLocation",
        "checkpoints/lab11"
    ) \
    .start()

print("Streaming działa")

spark.streams.awaitAnyTermination()