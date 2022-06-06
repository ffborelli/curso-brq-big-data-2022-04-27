# Partições Hive

A partição de tabela Hive é uma maneira de dividir uma tabela grande em tabelas lógicas menores com base em uma ou mais chaves de partição. Essas tabelas lógicas menores não são visíveis para os usuários e os usuários ainda acessam os dados de apenas uma tabela.

A cláusula PARTITIONED BY é utilizada para criar partições.

## criando a tabela

```
    CREATE TABLE zipcodes(
    RecordNumber int,
    Country string,
    City string,
    Zipcode int)
    PARTITIONED BY(state string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
```

## copiando dados para o HDFS

```
    hdfs dfs -put zipcodes.csv /user/cloudera
```

## carregando dados para a tabela do Hive

```
    LOAD DATA INPATH '/user/cloudera/zipcodes.csv'
```

## mostrar as partições

```
    SHOW PARTITIONS zipcodes;
```

