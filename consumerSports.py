from kafka import KafkaConsumer
from const import *
import sys

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
try:
  topic = "Sports"
except:
  print ('Usage: python3 consumer Sports')
  exit(1)
  
consumer.subscribe([topic])
for msg in consumer:
    print (msg.value)
