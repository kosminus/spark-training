from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("yarn") \
    .appName("dataframes") \
    .getOrCreate()

invoices_DF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("gs://sparktraining123/invoices.csv")

invoices_DF.show(2)

invoices_DF = invoices_DF.filter("InvoiceNo == 536365")

invoices_DF.write \
    .mode("Overwrite") \
    .parquet("gs://sparktraining123/invoices_parquet")
