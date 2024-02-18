# from pyspark import SparkContext, SparkConf
import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# print(sys.executable)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# spark=SparkSession.builder.config("spark.driver.host", "localhost").appName('appname').getOrCreate()
spark=SparkSession.builder.master("local").appName('appname').getOrCreate()

print("spark version ------ ", spark.version)
data = [
    ("Eden", "Hazard", 32),
    ("Eden", "Hazard", 30),
    ("Frank", "Lampard", 45),
    ("Petr", "Cech", 48),
    ("Petr", "Cech", 42),
]

columns = ["firstname", "lastname", "age"]
df = spark.createDataFrame(data=data, schema=columns)

print(df.show())

print(df.groupBy('firstname', 'Lastname').agg(func.avg('age')).show())

# print(df.show(truncate=True))

# conf = SparkConf().setAppName("appName").setMaster("master")

# sc = SparkContext(conf=conf)
