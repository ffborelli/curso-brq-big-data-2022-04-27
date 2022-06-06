# Bucketing Hive

O bucketing no Hive é uma técnica de organização de dados. É semelhante ao particionamento no Hive com uma funcionalidade adicional que divide grandes conjuntos de dados em partes mais gerenciáveis, conhecidas como buckets. 

## criando a tabela hive

```
    create table emp_demo 
        (id int, name string , department string)    
            row format delimited    
            fields terminated by ',' ;   
```

## criando a tabela hive bucketing

```
    create table emp_bucket
        (id int, name string , department string)    
            clustered by (id) into 2 buckets  
            row format delimited    
            fields terminated by ',' ;     
```

## copiando dados para o HDFS

```
    hdfs dfs -put employees.csv /user/cloudera
```

## carregando dados para a tabela stage do Hive

```
    LOAD DATA INPATH '/user/cloudera/employees.csv' OVERWRITE INTO TABLE emp_demo ;
```

## Ativar modo Bucket 

```
    set hive.enforce.bucketing = true; 
```

## carregando dados para a tabela particionada do Hive

```
   insert overwrite table emp_bucket select * from emp_demo; 
```


