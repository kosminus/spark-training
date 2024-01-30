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


#read from file
invoices_DF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("../data/input/invoices.csv")

#selecting

invoices_DF.select("InvoiceNo", "StockCode").show(2)

invoice_no_column:Column = invoices_DF["InvoiceNo"]
invoices_DF.select(invoice_no_column).show(2)
invoices_DF.select(F.col("CustomerID"))
invoices_DF.select(F.expr("lower('Description')").alias("new_description")).show(2)
invoices_DF.selectExpr("InvoiceNo", "lower('Description') as description").show(2)


#filtering

invoices_DF.filter(F.col("InvoiceNo") == 536365).show(2)

#filtering with multiple conditions

invoices_DF.filter((F.col("UnitPrice") > 3) & (F.col("UnitPrice") < 5)).show(2)


#adding columns
invoices_DF=invoices_DF.withColumn("new_column1", F.lit("ceva"))

invoices_DF = invoices_DF.withColumn("new_column", F.lit(None))

#renaming columns
# invoices_DF:DataFrame = invoices_DF.withColumnRenamed("InvoiceNo", "new_column")


#aggregations
invoices_DF.groupBy("Country").count().show(2)

invoices_DF.groupBy("Country").agg(
    F.sum("Quantity").alias("total_quantity"),
    F.avg("UnitPrice").alias("average_price")
).orderBy(F.col("total_quantity").desc_nulls_last()).show(2)


#nulls
invoices_DF.filter(F.col("CustomerID").isNull()).show(2)
invoices_DF.na.fill(0).show(2)
invoices_DF.na.drop().show(2)

invoices_DF.na.fill("0", subset=['CustomerID','Description']).show(2)
invoices_DF.select(F.coalesce("CustomerID", F.lit(9999))).show(2)


#deduplication
# invoices_DF.dropDuplicates(["InvoiceNo"]).show(2)


# window_spec = Window.partitionBy("InvoiceNo").orderBy(F.col("InvoiceNo").desc_nulls_last())
# invoices_dedup_Df = invoices_DF.withColumn("row_number", F.row_number().over(window_spec))
# invoices_dedup_Df.filter(F.col("row_number") == 1)


#window functions
#aggregate functions
window_spec = Window.partitionBy("InvoiceNo")
invoices_DF.withColumn("total_quantity", F.sum("Quantity").over(window_spec))


invoices_DF.selectExpr("max(Quantity) OVER (PARTITION BY InvoiceNo) AS max", "Country").distinct().show(5)


time.sleep(500)