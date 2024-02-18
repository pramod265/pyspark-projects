from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def parseInput(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])


if __name__ == '__main__':
    # Create a SparkSession
    spark = SparkSession. \
        builder.appName("cassandraExample")\
        .config("spark.cassandra.connection.host", '127.0.0.1')\
        .getOrCreate()

    # Build RDD on top of users data file
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/mongodb/movies.user")

    users = lines.map(parseInput)

    userDataset = spark.createDataFrame(users)

    userDataset.write.format("org.apache.spark.sql.cassandra")\
        .mode("append")\
        .options(table="user", keyspace="moviesdata")\
        .save()
