from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import *
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("test_UDF").getOrCreate()


# Spark UDFs example
def cubed(x):
    return x * x * x


spark.udf.register("cubed", cubed, LongType())
spark.range(1, 9).createOrReplaceTempView("udf_test")
spark.sql("select id, cubed(id) as cubed_id from udf_test;").show()


# Pandas UDF Example
def cubed_for_pd(x: pd.Series) -> pd.Series:
    return x * x * x


cubed_udf = pandas_udf(cubed_for_pd,returnType=LongType())

x = pd.Series([2, 1, 4])

print(cubed_for_pd(x))

# now creating spark DF

df = spark.range(1, 9)
df.select("id", cubed_udf(col("id"))).show()
