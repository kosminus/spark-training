import time

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# SparkConf
#
# conf = SparkConf().setMaster("local[*]").setAppName("test")
# conf.set("spark.driver.memory", "1g")
#
# print(conf.get("spark.master"))
#
# SparkContext
# sc = SparkContext(conf=conf)
# print(sc.version)
# print(sc.applicationId)
# print(sc.defaultParallelism) #nr of cores
# print(sc.uiWebUrl)
#
# for conf in sc.getConf().getAll():
#     print(conf)

SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dataframes") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

spark.sparkContext.applicationId


spark.createDataFrame([{'name': 'Alice', 'age': 1},]).show()
spark.sql("select 1 as id").show()


time.sleep(500)