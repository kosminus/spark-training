import time

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame, Column, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType, Row

conf = SparkConf()
# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
jar_path = "../jdbc/mysql-connector-j-8.3.0.jar"
conf.set("spark.driver.extraClassPath", jar_path)


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("datasources") \
    .config(conf=conf) \
    .getOrCreate()

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# customers_DF = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:mysql://192.168.95.201:3306/classicmodels") \
#     .option("dbtable", "customers") \
#     .option("user", "spark") \
#     .option("password", "spark") \
#     .load()
#
# customers_DF.show()
#
# customers_DF = customers_DF.filter("country = 'USA'")
# #Overwrite si Append
# customers_DF.write \
#     .partitionBy("country") \
#     .mode("Overwrite") \
#     .save("../data/output/customers_jdbc")


# customers_DF = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:mysql://localhost:3306/classicmodels") \
#     .option("dbtable", "(select * from customers) as customers") \
#     .option("user", "spark") \
#     .option("password", "spark") \
#     .option("numPartitions", 10) \
#     .option("partitionColumn", "customerNumber") \
#     .option("lowerBound", 1) \
#     .option("upperBound", 500) \
#     .load()
#
# customers_DF.write \
#     .partitionBy("country") \
#     .mode("Overwrite") \
#     .save("../data/output/customers_jdbc")

countries = [
    "France", "USA", "Australia", "Norway", "Poland", "Germany", "Spain",
    "Sweden", "Denmark", "Singapore", "Portugal", "Japan", "Finland", "UK",
    "Ireland", "Canada", "Hong Kong", "Italy", "Switzerland", "Netherlands",
    "Belgium", "New Zealand", "South Africa", "Austria", "Philippines"
]
column="country"
predicates  = [f"{column} = '{country}'" for country in countries]

customers_DF = spark.read \
    .jdbc("jdbc:mysql://localhost:3306/classicmodels", "customers", predicates=predicates, properties={"user": "spark", "password": "spark"})


customers_DF.write \
    .partitionBy("country") \
    .mode("Overwrite") \
    .save("../data/output/customers_jdbc")
time.sleep(1000)
