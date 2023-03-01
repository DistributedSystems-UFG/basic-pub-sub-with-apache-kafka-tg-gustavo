from kafka import KafkaProducer
from const import *
import sys

try:
    topic1 = "Sports"
    topic2 = "Tech"
    topic3 = "Fishing"
except:
    print ('Usage: python3 producer <topic_name>')
    exit(1)
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(15):
    msg = 'Noticia n.' + str(i+1) + ' sobre o tema: ' + topic1
    print ('Sending message: ' + msg)
    producer.send(topic1, value=msg.encode())

for i in range(25):
    msg = 'Noticia n.' + str(i+1) + ' sobre o tema: ' + topic2
    print ('Sending message: ' + msg)
    producer.send(topic2, value=msg.encode())

for i in range(5):
    msg = 'Noticia n.' + str(i+1) + ' sobre o tema: ' + topic3
    print ('Sending message: ' + msg)
    producer.send(topic3, value=msg.encode())


producer.flush()
