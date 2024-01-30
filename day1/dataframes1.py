import time

from pyspark.sql import SparkSession, DataFrame, Column
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

invoices_DF.printSchema()
# invoices_DF.show(100,truncate=False)

# rows_5= invoices_DF.take(5)


invoices_schema = StructType([
    StructField("InvoiceNo", IntegerType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True)
])

# invoices_DF = spark.read \
#     .format("csv") \
#     .option("header", "true") \
#     .option("TimeStampFormat", "M/d/yyyy hh:mm") \
#     .schema(invoices_schema) \
#     .load("../data/input/invoices.csv")
#
#
# invoices_DF.printSchema()
# invoices_DF.show(2,truncate=False)


#create from rdd
columns = ["id", "name", "age", "city"]
data = [(1, "john", 30, "new york"),(2, "bob", 25, "chicago"),(3, "anna", 20, "prague")]
rdd = spark.sparkContext.parallelize(data)
people_df = rdd.toDF(columns)
people_df.printSchema()
people_df.show()


# create from list of rows
DataFrame
Row
Column
people_schema = people_df.schema
print(people_schema)
people_df.printSchema()


rows = [Row(id=1, name="john", age=30, city="new york"),
        Row(id=2, name="bob", age=25, city="chicago"),
        Row(id=3, name="anna", age=20, city="prague")]

people_df = spark.createDataFrame(rows)
people_df.printSchema()
people_df.show()




time.sleep(5000)