import random
import time
from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, rand

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("joins") \
    .getOrCreate()

customers_DF = spark.read.load("../data/output/customers")
orders_DF = spark.read.load("../data/output/orders")

#broadcast hint
# customers_DF.join(orders_DF.hint("broadcast"), customers_DF["id"] == orders_DF["customer_id"], "inner").explain("cost")
# simple, extended , cost , codegen


#PushedFilters
# customers_DF.filter(customers_DF["id"] > 5).join(orders_DF, customers_DF["id"] == orders_DF["customer_id"], "inner").explain()
# customers_DF.filter(customers_DF["id"] > 5).join(orders_DF, customers_DF["id"] == orders_DF["customer_id"], "inner").count()


#column pruning
customers_DF.join(orders_DF, customers_DF["id"] == orders_DF["customer_id"], "inner").select("name", "purchase_date").explain()


#nulls in joins

# #nulls in joins
# without_nulls_df = orders_DF.join(customers_DF, orders_DF["customer_id"] == customers_DF["id"]).count()
# with_nulls_df = orders_DF.join(customers_DF, orders_DF["customer_id"].eqNullSafe(customers_DF["id"])).count()
# print(f"Without nulls: {without_nulls_df}, with nulls: {with_nulls_df}")



# deactivate broadcast joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.conf.set("spark.sql.adaptive.enabled", "true")


#
time.sleep(500)

spark.sparkContext.version


