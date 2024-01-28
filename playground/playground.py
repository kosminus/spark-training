from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()


spark.createDataFrame([("a", 1), ("b", 2)]).show(2)