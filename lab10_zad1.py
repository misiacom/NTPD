import pandas as pd
from pyspark.sql import SparkSession

csv_path = "data/vegetables.csv"
parquet_path = "data/vegetables.parquet"

df_pandas = pd.read_csv(csv_path)

df_pandas.to_parquet(parquet_path)

print("plik parquet się utworzył")


spark = SparkSession.builder \
    .appName("SparkSQL_Parquet") \
    .getOrCreate()

df = spark.read.parquet(parquet_path)

print("podgląd danych")
df.show(10)

print("schemat danych")
df.printSchema()

spark.stop()