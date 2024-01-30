import time

from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dataframes") \
    .enableHiveSupport() \
    .getOrCreate()


invoices_DF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("../data/input/invoices.csv")


spark.sql("create database if not exists kosmin").show()
spark.sql("show databases").show()
spark.sql("create table if not exists test (id int, name string)").show()
spark.sql("show tables").show()

invoices_DF.createOrReplaceTempView("invoices")

spark.sql("SELECT * FROM invoices").show(2)

spark.sql("create table kosmin.invoices as select * from invoices").show()
spark.sql("describe extended kosmin.invoices").show(truncate=False)


time.sleep(500)