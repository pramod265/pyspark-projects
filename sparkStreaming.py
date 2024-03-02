from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("sparkStreaming").getOrCreate()

# Define Input Socket
lines = (spark.readStream.format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load())

# Transform data split data into words
words = lines.select(split(col("value"), "\\s").alias("word"))

# Get the count of published words
counts = words.groupBy("word").count()

# Define the checkpoint directory (helps in failure recovery)
checkpoint_dir = "D:/Projects/pyspark-code/checkpoint/"

# Start streaming defining required configuration
stream_query = (counts.writeStream.format("console").
                outputMode("complete").
                trigger(processingTime="1 second")
                .option("checkpointlocation", checkpoint_dir).start())

stream_query.awaitTermination()
