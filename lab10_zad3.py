import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Asus\PycharmProjects\SparkLab09\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Asus\PycharmProjects\SparkLab09\venv\Scripts\python.exe"
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSQL_Queries") \
    .getOrCreate()

df_sales = spark.read.csv(
    "data/vegetables.csv",
    header=True,
    inferSchema=True
)

df_sales.createOrReplaceTempView("sales")

products_data = [
    (102900005117056, "Tomato"),
    (102900005115960, "Potato"),
    (102900005115823, "Carrot"),
    (102900005115908, "Onion"),
    (102900005115779, "Cucumber"),
    (102900005118824, "Pepper")
]

df_products = spark.createDataFrame(
    products_data,
    ["Item Code", "Product_Name"]
)

df_products.createOrReplaceTempView("products")

print("agregacje")

aggregation_query = spark.sql("""
SELECT
    COUNT(*) AS liczba_transakcji,
    AVG(`Quantity Sold (kilo)`) AS srednia_ilosc,
    SUM(`Quantity Sold (kilo)`) AS suma_ilosci
FROM sales
""")

aggregation_query.show()

print("GROUP BY")

group_query = spark.sql("""
SELECT
    `Sale or Return`,
    COUNT(*) AS liczba_transakcji,
    AVG(`Quantity Sold (kilo)`) AS srednia_sprzedaz
FROM sales
GROUP BY `Sale or Return`
""")

group_query.show()

print("WHERE")

where_query = spark.sql("""
SELECT *
FROM sales
WHERE `Quantity Sold (kilo)` > 1
LIMIT 10
""")

where_query.show()

print("JOIN")

join_query = spark.sql("""
SELECT
    s.`Item Code`,
    p.Product_Name,
    s.`Quantity Sold (kilo)`,
    s.`Unit Selling Price (RMB/kg)`
FROM sales s
JOIN products p
ON s.`Item Code` = p.`Item Code`
LIMIT 10
""")

join_query.show(10, truncate=False)

join_result = join_query.toPandas()

join_result.to_csv(
    "output/join_result.csv",
    index=False
)

print("zapisano do output/join_result.csv")

spark.stop()