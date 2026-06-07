from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("LAB11_StructuredStreaming") \
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

df = df.filter(col("amount") > 0)

print("jest strumieniowy?")
print(df.isStreaming)

print("schemat")
df.printSchema()

query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

print("streaming działa")

query.awaitTermination()