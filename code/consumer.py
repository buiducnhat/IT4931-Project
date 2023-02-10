from kafka import KafkaConsumer
import pydoop.hdfs as hdfs
import time
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import json
# Create spark session
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

consumer = KafkaConsumer('GasData',bootstrap_servers=['10.157.60.165:9092'])

schema = StructType([
    StructField("change", StringType(), True),
    StructField("date", StringType(), True),
    StructField("fuelType", StringType(), True),
    StructField("percentChange", StringType(), True),
    StructField("x", StringType(), True),
    StructField("y", StringType(), True)
    # Add more fields as required
])

for message in consumer:
    msg = message.value.decode("utf-8") 
    tmp = json.loads(msg)
    print(tmp)
    df = spark.createDataFrame(tmp, schema)
    df.show()
    df.write.format('csv') .mode('append').option("header", "true").save("hdfs://10.157.60.165:9000/GasData/dataCsv")