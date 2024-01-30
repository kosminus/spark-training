import time
from time import sleep

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dataframes") \
    .getOrCreate()


df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("spark.driver.memory", "1g") \
    .option("spark.executor.memory", "1g") \
    .load("../data/input/invoices.csv")


print(df.rdd.getNumPartitions())
df  = df.repartition(20)
print(df.rdd.getNumPartitions())

df.cache()
df.take(10)

df.persist(StorageLevel.MEMORY_ONLY)

time.sleep(1000)