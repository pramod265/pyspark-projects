from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName('sparkSQL').getOrCreate()
print(spark.version)
# Creating Dataframe from CSV
data = spark.read.csv('operations_management.csv', inferSchema=True, header=True)

# different way to create Dataframe from CSV
# data2 = spark.read.format('csv').option('inferSchema', True)\
#         .option('header', True).option("path", "operations_management.csv").load()

print(data.printSchema())
print(data.show())

# Applying transformation on Dataframe directly using dataframe functions
data2 = data.select('industry', 'value')\
    .filter((data.value > 1000) & (data.industry != 'total'))\
    .orderBy(desc('value'))

print(data2.printSchema())
print(data2.show(5))

# Creating temp view to run spark/sql queries
data.createTempView('vw_opsData')

spark.sql("select industry, value from vw_opsData where value > 1000 and industry != 'total' order by value desc")\
    .show(5)
