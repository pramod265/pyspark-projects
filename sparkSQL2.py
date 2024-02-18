from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("sparkSQL2").enableHiveSupport().getOrCreate()

data = spark.read.csv("operations_management.csv", header=True, inferSchema=True)

print(data.printSchema())

# creating Temp view
data.createOrReplaceTempView("test")
data2 = spark.sql("Select * from test;")
print(data2.show())

# creating global Temp view
data.createOrReplaceGlobalTempView("test_view")
data3 = spark.sql("Select * from global_temp.test_view")
print(data3.show())

# view metadata of DB
print(spark.catalog.listDatabases())

# view tables in DB
print(spark.catalog.listTables())
print(spark.catalog.listTables())

# drop view
spark.catalog.dropTempView("test")
spark.catalog.dropGlobalTempView("test_view")

# view tables in DB
print(spark.catalog.listTables())
