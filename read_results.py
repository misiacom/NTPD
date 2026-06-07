from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadResults") \
    .getOrCreate()

df = spark.read.parquet(
    "data/output_stream"
)

print("wyniki z parquet")

df.show(truncate=False)

spark.stop()