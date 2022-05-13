from kafka import KafkaConsumer

from json import loads

import mysql.connector

db = mysql.connector.connect(host='mysql', user='root', \
    password='root', port=3306, database='brq_python')

print('Bancccccccccccccccccccccccccccccccccccccccccccc')

cursor = db.cursor()
#cursor.execute('use brq_python')
cursor.execute('CREATE TABLE IF NOT EXISTS minha_media (media DECIMAL (6,2) )')

consumer= KafkaConsumer(
    'meu-topico-legal',
    bootstrap_servers=['singlenode_kafka_1:29092'],
    value_deserializer= lambda x: loads( x.decode('utf-8') )
)

soma = 0
contador = 0

try:

    for msg in consumer:
        print(msg.value)

        soma += msg.value
        contador += 1
        media = soma/contador

        print(f'a media e {media}')
        cursor.execute(f'INSERT INTO minha_media (media) VALUES ({media})')
        db.commit()

except KeyboardInterrupt:
    print('Fechando a conexao')
    cursor.close()


