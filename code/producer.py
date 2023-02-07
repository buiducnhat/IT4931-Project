from kafka import KafkaProducer
from json import dumps
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda K:dumps(K).encode('utf-8'))

# Open the JSON file
with open('./crawl-gas/natural-gas.json', 'r') as f:
    # Read the file line by line
    for line in f:
        # Parse the JSON data
        data = json.loads(line)
        # Send the data to the Kafka topic
        producer.send('TutorialTopic', value=data)

# Flush the producer to ensure all messages are sent
producer.flush()

# for e in range(100):
#     data = {'number' : e}
#     producer.send('TutorialTopic', value=data)