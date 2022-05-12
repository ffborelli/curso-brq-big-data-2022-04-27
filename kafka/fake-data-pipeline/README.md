# fake-data-pipeline
Data Generators -> Kafka -> Spark Streaming -> PostgreSQL -> Grafana
![fake-data-pipleine](https://user-images.githubusercontent.com/31606119/108158077-88570680-7127-11eb-9339-910b5a8393aa.png)

### 1. fake data -> Kafka
- Enviar dados gerados aleatoriamente para o tópico Kafka (Topics: iot, words)
- [data-generators/iot_devices.py](https://github.com/JHLeeeMe/fake-data-pipeline/blob/master/data-generators/iot_devices.py)
- [data-generators/words.py](https://github.com/JHLeeeMe/fake-data-pipeline/blob/master/data-generators/words.py)
- [data-generators/seoul_bike_api.py](https://github.com/JHLeeeMe/fake-data-pipeline/blob/master/data-generators/seoul_bike_api.py) (Not fake!)

### 2. Spark Structured Streaming -> PostgreSQL
- iot, words, bike (tópicos)
- Filtrar e agregar timestamps
- [StreamHandler.scala](https://github.com/JHLeeeMe/fake-data-pipeline/blob/master/StreamHandler/src/main/scala/StreamHandler.scala)

### 3. Grafana
- PostgreSQL에 담겨지는 데이터들을 시각화

**IOT Dashboard**
![IOT_Dashboard](https://user-images.githubusercontent.com/31606119/98818999-ab5e8400-246f-11eb-8b6c-cc4a220758bd.png)

**Words Dashboard**
![Words_Dashboard](https://user-images.githubusercontent.com/31606119/98819004-ac8fb100-246f-11eb-9f1e-99548f1cd30f.png)

**Bike Dashboard**
![Bike_Dashboard](https://user-images.githubusercontent.com/31606119/102494317-d0ba5f80-40b7-11eb-8759-719b395f322b.png)

---

### 1. git clone
```bash
git clone https://github.com/JHLeeeMe/fake-data-pipeline.git
```
### 2. Run docker-compose
```bash
cd fake-data-pipeline && docker-compose up -d

# Check
docker-compose ps
```

Containers a serem executados

**Spark**: master(10.0.0.10), slave-1, slave-2, slave-3 (10.0.0.11 ~ 10.0.0.13)  
**Kafka**: kafka-single-node(10.0.0.30)  
**PostgreSQL**: postgresql(10.0.0.40)  
**Grafana**: grafana(10.0.0.50)

### 3. Run data-generators
master container에서 python script들을 실행하자.
```bash
docker exec -it master /bin/bash

# master container
master$ cd ~/fake-data-pipeline
master$ pip3 install -r requirements.txt
master$ python3 data-generators/iot_devices.py <home || seoul>
```


### 4. Run spark-submit
3번 과정에 의해 fake 데이터들이 iot, words 토픽으로 보내지는 중  
```StreamHandler```를 실행시켜 데이터를 정제 후 PostgreSQL로 저장.  
spark-submit 실행 스크립트를 만들어두었다.
```bash
docker exec -it master /bin/bash

# master container
master$ cd /root/fake-data-pipeline/StreamHandler
master$ ./run-spark-submit.sh
```

### 5. Grafana
No navegador ```localhost:3000```로 접속  
(초기 id: admin, password: admin)  
data source를 postgresql


