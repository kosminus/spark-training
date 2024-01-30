import time

from pyspark.sql import SparkSession, DataFrame, Column, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType, Row

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dataframes") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.legacy.timeParserPolicy" , "LEGACY") \
    .getOrCreate()


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dataframes") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


customers_DF = spark.read \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .json("../data/input/customers.json")

orders_DF = spark.read \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .json("../data/input/orders.json")

products_DF = spark.read \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .json("../data/input/products.json")


#inner join
customers_DF.join(orders_DF, customers_DF["id"] == orders_DF["customer_id"], "inner").show()
#
# #left outer join
# customers_DF.join(orders_DF, on=customers_DF["id"] == orders_DF["customer_id"], how="left_outer").show(2)
# customers_DF.filter(customers_DF["id"] == 1).join(orders_DF, on=customers_DF["id"] == orders_DF["customer_id"], how="left_outer").show(2)
#
#
# #same column name
# products_DF = products_DF.withColumnRenamed("id", "product_id")
#
#
# orders_DF.join(products_DF, "product_id", "inner").show(2)
# orders_DF.join(products_DF, on=orders_DF["product_id"] == products_DF["product_id"], how="inner") \
#     .drop(orders_DF["product_id"]).show(2)
#
#
# #join with multiple conditions
# joined_DF = products_DF.join(orders_DF, (orders_DF["product_id"] == products_DF["product_id"]) &
#                             (orders_DF["quantity"] > 10), "left") \
#      .drop(products_DF["product_id"])
#
#
# #A semi join returns values from the left side of the relation that has a match with the right
# #no right side columns are added to the result
# #semi join
# joined_DF = orders_DF.join(products_DF, orders_DF["product_id"] == products_DF["product_id"], "left_semi")
# joined_DF.show(2)
#
#
# #anti join
# #An anti join returns values from the left relation that has no match with the right.
# #no right side columns are added to the result
# joined_DF = products_DF.join(orders_DF, orders_DF["product_id"] == products_DF["product_id"], "left_anti")
# joined_DF.show(2)
#
#


customers_DF.coalesce(1).write.mode("overwrite").parquet("../data/output/customers")
orders_DF.coalesce(1).write.mode("overwrite").parquet("../data/output/orders")
products_DF.coalesce(1).write.mode("overwrite").parquet("../data/output/products")

time.sleep(400)