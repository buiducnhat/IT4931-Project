from kafka import KafkaConsumer
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import json

sc = SparkContext('local')
spark = SparkSession(sc)

consumer = KafkaConsumer('GasData',bootstrap_servers=['localhost:9092'])

schema = StructType([
    StructField("change", StringType(), True),
    StructField("date", StringType(), True),
    StructField("fuelType", StringType(), True),
    StructField("percentChange", StringType(), True),
    StructField("x", StringType(), True),
    StructField("y", StringType(), True)
])

for message in consumer:
    msg = message.value.decode("utf-8") 
    tmp = json.loads(msg)
    df = spark.createDataFrame(tmp, schema)
    df.show()
    df.write.format('csv') .mode('append').option("header", "true").save("hdfs://localhost:9000/GasData/dataCsv")
