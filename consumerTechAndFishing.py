from kafka import KafkaConsumer
from const import *
import sys

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
try:
  topics = ["Tech", "Fishing"]
except:
  print ('Usage: python3 consumer Tech and Fishing')
  exit(1)
  
consumer.subscribe(topics)
for msg in consumer:
    print (msg.value)
