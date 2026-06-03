from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("SparkSQL_CSV") \
    .getOrCreate()

df_csv = spark.read.csv(
    "data/vegetables.csv",
    header=True,
    inferSchema=True
)

print("podgląd danych")
df_csv.show(10)

print("schemat danych")
df_csv.printSchema()

df_csv.createOrReplaceTempView("vegetables")

print("widok tymczasowy się utworzył")

query_result = spark.sql("""
    SELECT *
    FROM vegetables
    LIMIT 10
""")

print("wynik SQL")
query_result.show()

spark.stop()