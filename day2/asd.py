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

customers_DF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://192.168.95.201:3306/classicmodels") \
    .option("dbtable", "customers") \
    .option("user", "spark") \
    .option("password", "spark") \
    .load()

customers_DF.show(2)

orders_DF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://192.168.95.201:3306/classicmodels") \
    .option("dbtable", "orders") \
    .option("user", "spark") \
    .option("password", "spark") \
    .load()

orders_DF.show(2)

joined = customers_DF.join(orders_DF, customers_DF["customerNumber"] == orders_DF["customerNumber"], "inner")


orders_DF.createOrReplaceTempView("vw_orders")
customers_DF.createOrReplaceTempView("vw_customers")

joined = spark.sql(f"""
select count(1) from vw_orders a inner join vw_orders b on a.customerNumber = b.customerNumber """)
joined.show()