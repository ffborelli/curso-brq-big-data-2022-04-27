# Partições Hive

A partição de tabela Hive é uma maneira de dividir uma tabela grande em tabelas lógicas menores com base em uma ou mais chaves de partição. Essas tabelas lógicas menores não são visíveis para os usuários e os usuários ainda acessam os dados de apenas uma tabela.

A cláusula PARTITIONED BY é utilizada para criar partições.

## criando a tabela stage

```
    CREATE TABLE zipcodes_stage(
    RecordNumber int,
    Country string,
    City string,
    Zipcode int,
    state string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
```

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
    hdfs dfs -put zipcodes20.csv /user/cloudera
```

## carregando dados para a tabela stage do Hive

```
    LOAD DATA INPATH '/user/cloudera/zipcodes20.csv' OVERWRITE INTO TABLE zipcodes_stage ;
```
## Particionamento variável

O modo não estrito significa que permitirá que toda a partição seja dinâmica. Também pode ser chamado de particionamento variável

```
    set hive.exec.dynamic.partition.mode=nonstrict;
```

## carregando dados para a tabela particionada do Hive

```
 insert overwrite table zipcodes partition(state)
        select
			state,
            RecordNumber,
            Country,
            City,
            Zipcode
        from zipcodes_stage;
```

## mostrar as partições

```
    SHOW PARTITIONS zipcodes;
```

