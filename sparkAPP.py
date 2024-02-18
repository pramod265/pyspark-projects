import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
import pyspark.sql.functions as func

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Defining Spark Session
spark = SparkSession.builder.appName("FirstAPP").getOrCreate()

# Creating Schema for Dataframe
my_schema = StructType([
                StructField("user_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("friends", IntegerType(), True)
            ])

# Creating dataframe from csv file
# people = spark.read.csv("friends_data.csv",my_schema)
people = spark.read.format("csv").schema(my_schema).option("path", "friends_data.csv").load()


output = people.select(people.user_id, people.name,
                       people.age, people.friends)\
    .where(people.age < 30)\
    .withColumn('insert_ts', func.current_timestamp())\
    .orderBy(people.user_id)

print(output.count())

output.createTempView("vw_peoples")

spark.sql("select name, age, friends, insert_ts from vw_peoples").show()