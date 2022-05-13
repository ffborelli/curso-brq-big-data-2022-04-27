# instalação
# tutorial base: https://medium.com/trainingcenter/apache-kafka-codifica%C3%A7%C3%A3o-na-pratica-9c6a4142a08f

### clonar repositório confluent 

```
git clone https://github.com/confluentinc/cp-docker-images.git
```

### executar kafka no docker

Após clonado, navegue até a pasta cp-docker-images/examples/kafka-single-node. Esta pasta conterá o seguinte arquivo docker-compose.yml

```
cd kafka-single-node
```

### criando tópico 

entrar dentro do container kafka-single-node_kafka_1 via console

```
docker exec -it singlenode_kafka_1 /bin/bash
```

```
kafka-topics --create --topic meu-topico-legal --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
```

###  se quiser confirmar se o Topic foi criado, execute o comando abaixo:

entrar dentro do container singlenode_kafka_1 via console

```
docker exec -it singlenode_kafka_1 /bin/bash
```

```
kafka-topics --describe --topic meu-topico-legal --bootstrap-server localhost:9092
```

###  Produzindo mensagens com o Producer

Enviando 100 mensagens para o tópico meu-topico-legal

```
bash -c "seq 100 | kafka-console-producer --request-required-acks 1 --broker-list localhost:29092 --topic meu-topico-legal && echo 'Produced 100 messages.'"
```

###  Consumindo mensagens com o Consumer

Consumindo mensagens do tópico meu-topico-legal

```
kafka-console-consumer --bootstrap-server localhost:29092 --topic meu-topico-legal --from-beginning --max-messages 100
```


### criando tópico (kafka-python-topic)

```
kafka-topics --create --topic kafka-python-topic --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
```

### deletando um tópico (kafka-python-topic)

kafka-topics --delete --topic kafka-python-topic --bootstrap-server localhost:9092

## Parte 2

### Para entrar dentro do container do nó Kafka

```
docker exec -it singlenode_kafka_1 /bin/bash
```

### permitir publicar mensagens em um tópico Kafka

```
kafka-console-producer --broker-list localhost:29092 --topic meu-topico-legal
```

### subscrição com grupo Kafka

Quando escalamos a aplicação consumidora, compartilhando o mesmo group-id, todas se tornarão parte de um consumer group. Com isso, cada consumer nesse grupo irá receber mensagens de uma ou mais partições do tópico subscrito.

Rodar o comando abaixo em dois terminais diferentes (ambos vão pertencer ao mesmo GRUPO).

Enviar mensagens para o tópico meu-topico-legal. Sempre irá para o mesmo consumidor.

```
kafka-console-consumer --bootstrap-server localhost:29092 --topic meu-topico-legal --group grupo1
```

### Acrescentar partições em um tópico

```
kafka-topics --alter --bootstrap-server localhost:9092 --topic meu-topico-legal --partitions 10
```

### Mensagens com chave (para guardar sempre na mesma chave)

Kafka sempre irá armazenar a mesma chave na mesma partição.

#### Consumer

Separador de chaves é a vírgula

Abrir dois terminais com o mesmo comando abaixo.

```
kafka-console-consumer --bootstrap-server localhost:29092 --topic meu-topico-legal --group grupo1 --property "print.key=true" --property "key.separator=,"
```

#### Producer

Separador de chaves é a vírgula.

Publicar mensagens no tópico abaixo:

```
kafka-console-producer --broker-list localhost:29092 --topic meu-topico-legal --property "parse.key=true" --property "key.separator=,"
```

# Parte 3

## subir modo cluster

cd kafka-cluster
docker-compose up

## entrar dentro de um container node Kafka

docker exec -it kafka-cluster_zookeeper-1_1 /bin/bash

## criar novo tópico

kafka-topics --create --topic meu-topico-legal --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server localhost:19092

## verificar se tópico foi criado

```
kafka-topics --describe --topic meu-topico-legal --bootstrap-server localhost:19092
```

### subscrição tópico kafka (19092)

```
kafka-console-consumer --bootstrap-server localhost:19092 --topic meu-topico-legal 
```

###  Produzindo mensagens com o Producer -> ordem (19092,29092, 39092)

```
bash -c "seq 100 | kafka-console-producer --request-required-acks 1 --broker-list localhost:19092,localhost:29092,localhost:39092 --topic meu-topico-legal && echo 'Produced 100 messages.'"
```

# Documentação

### Criar tópico
kafka-topics --bootstrap-server localhost:9092 --topic <nome_topico> --create --partitions 3 --replication-factor 1

### Acrescentar partições em um tópico
kafka-topics --alter --bootstrap-server localhost:9092 --topic <nome_topico> --partitions <qtd>

### Listar tópicos
kafka-topics --bootstrap-server localhost:9092 --list

### Detalhes do tópico
kafka-topics --bootstrap-server localhost:9092 --topic <nome_topico> --describe

### Deletar tópico (Não funciona no Windows) 
kafka-topics --bootstrap-server localhost:9092 --topic <nome_topico> --delete

### Enviar mensagem via linha de comando:
kafka-console-producer --broker-list 127.0.0.1:9092 --topic <nome_topico>

### Consumir mensagens via linha de comando:
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic <nome_topico>

### Consumir mensagens via linha de comando (desde o inicio):
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic <nome_topico> --from-beginning

### Consumir mensagens em grupo
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic <nome_topico> --group <group-name>

### Mostrar grupos
kafka-consumer-groups --bootstrap-server localhost:9092 --list

### Visualizar status das entregas (lag) por grupo:
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group-name>

### Reiniciar o offset do grupo para tópico específico
kafka-consumer-groups --bootstrap-server localhost:9092 --group <group-name> --reset-offsets --to-earliest --execute --topic <nome_topico>

### Reiniciar o offset do grupo para todos os tópicos
kafka-consumer-groups --bootstrap-server localhost:9092 --group <group-name> --reset-offsets --to-earliest --execute --all-topics