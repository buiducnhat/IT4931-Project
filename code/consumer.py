from kafka import KafkaConsumer
import pydoop.hdfs as hdfs
consumer = KafkaConsumer('TutorialTopic',bootstrap_servers=['localhost:9092'])
hdfs_path = 'hdfs://localhost:9000/GasData/natural-gas.txt'
 
for message in consumer:
   with hdfs.open(hdfs_path, 'at') as f:
      print(message.value)
      f.write(message.value.decode('utf-8'))