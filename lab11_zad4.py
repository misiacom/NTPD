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
    sum,
    window
)

spark = SparkSession.builder \
    .appName("LAB11_Windows_Watermark") \
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

fixed_window_summary = df.withWatermark(
    "event_time",
    "10 minutes"
).groupBy(
    window(
        col("event_time"),
        "10 minutes"
    ),
    col("category")
).agg(
    count("*").alias("events_count"),
    sum("amount").alias("total_amount")
)

sliding_window_summary = df.withWatermark(
    "event_time",
    "10 minutes"
).groupBy(
    window(
        col("event_time"),
        "10 minutes",
        "5 minutes"
    ),
    col("category")
).agg(
    count("*").alias("events_count"),
    sum("amount").alias("total_amount")
)

query_fixed = fixed_window_summary.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .queryName("fixed_window") \
    .start()

query_sliding = sliding_window_summary.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .queryName("sliding_window") \
    .start()

print("Streaming działa")

spark.streams.awaitAnyTermination()