from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

input_dir = "D:/Projects/pyspark-code/spark-stream-test/input/"
output_dir = "D:/Projects/pyspark-code/spark-stream-test/output/"
checkpoint_dir = "D:/Projects/pyspark-code/spark-stream-test/checkpoint/"

spark = SparkSession.builder.appName('spark-stream-sources').getOrCreate()

file_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("friends", IntegerType(), True),
])

input_df = spark.readStream.format("csv").schema(file_schema).load(input_dir)

# input_df = spark.readStream.csv(path=input_dir, schema=file_schema)

result_df = input_df.select("name", "friends").where(input_df.age < 30)

stream_query = (result_df.writeStream.format("csv")
                .option("path", output_dir)
                .option("checkpointlocation", checkpoint_dir)
                .start())

stream_query.awaitTermination()
