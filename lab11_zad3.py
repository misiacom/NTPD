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
    hour
)

spark = SparkSession.builder \
    .appName("LAB11_Transformations") \
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

# 1. Konwersja czasu
df = df.withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

df = df.filter(
    col("status") == "paid"
)

df = df.withColumn(
    "event_hour",
    hour(col("event_time"))
)

# 4. Wybór kolumn
df = df.select(
    "event_time",
    "event_hour",
    "user_id",
    "category",
    "amount"
)

summary = df.groupBy("category").agg(
    count("*").alias("events_count"),
    sum("amount").alias("total_amount")
)

query = summary.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

print("Streaming działa")

query.awaitTermination()