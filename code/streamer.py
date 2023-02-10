from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, count
from pyspark.sql.types import StringType,DoubleType

import sys

# Create spark session
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

KAFKA_TOPIC = "GasData"

# Define the schema for the incoming data
schema = StructType([
    StructField("change", StringType(), True),
    StructField("date", StringType(), True),
    StructField("fuelType", StringType(), True),
    StructField("percentChange", StringType(), True),
    StructField("x", StringType(), True),
    StructField("y", StringType(), True)
    # Add more fields as required
])

df = spark.read.csv("hdfs://10.157.60.165:9000/GasData/dataCsv/*.csv", header=True, schema=schema)

df.printSchema()
print('Số bản ghi %d' %df.count())
print(df.count())
# df.where()
df.show(20, False)

#list all fuel type
df.select('fuelType').distinct().show(20, False)
df2 = df.withColumn("y",df.y.cast('double'))
# get max y

if len(sys.argv) > 1:
    print('Số bản ghi %s' %sys.argv[1])
    print(df.where(col('fuelType') == sys.argv[1]).count())
    df.where(col('fuelType') == sys.argv[1]).show(20, False)