from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LAB11_StructuredStreaming") \
    .getOrCreate()

print("wersja sparka")
print(spark.version)

spark.stop()