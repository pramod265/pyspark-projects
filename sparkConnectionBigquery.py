from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "plexiform-timer-SA.json"


spark = SparkSession.builder.appName("spark_bq")\
    .config('spark.jars', 'spark-3.4-bigquery-0.36.1.jar')\
    .getOrCreate()

table = "plexiform-timer-301119.airflow_test.stock_market_data"
df = spark.read.format('bigquery').option('table', table).load()
print(df.printSchema())
# df.show(5)
df.createTempView("test_table")
spark.sql("Select open,high from test_table limit 10").show()
