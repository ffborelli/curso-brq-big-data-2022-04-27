from kafka import KafkaConsumer
from kafka import KafkaProducer,KafkaConsumer
from json import loads
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ArCondicionado").getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-single-node_kafka_1:29092") \
    .option("subscribe", "devices") \
    .load()
    
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


producer = KafkaProducer(bootstrap_servers='kafka-single-node_kafka_1:29092',
                        value_serializer=lambda v:str(v).encode('utf-8'))


consumer = KafkaConsumer(
    'kafka-python-topic',
     bootstrap_servers=['kafka-single-node_kafka_1:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

def liga_ar(avg):
    if avg >= 30 : 
        flag = 1
    else : 
        flag = 0
    if flag == 1 :
        print('\nLigar o ar condicionado !')
    return flag

from statistics import mean
list10 = []
for msg in consumer:
    print(msg)
    message = msg.value
    print(message)
    while len (list10) > 10 :
        list10.pop(0)
        media = mean(list10)
        flag = liga_ar(media)
        producer.send('devices', flag)
        print("Média: ",media)
    list10.append(message)