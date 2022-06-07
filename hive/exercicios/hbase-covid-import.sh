#!/bin/bash

# copiar o csv do COVID da nossa maquina para o HDFS
echo 'copiando o arquivo covid_19_data.csv para o HDFS'
hdfs dfs -put covid_19_data.csv /user/cloudera

# executar scripts hive para criar o banco de dados, tabelas e importacao dos dados

echo 'executando scripts de criacao de tabelas e ingestao de dados no HIVE'
hive -f script_hive.sql

# exportar os dados do Hive para um csv

echo 'extraindo dados do Hive e exportando um CSV'
hive -e 'SELECT * FROM covid.covid_19 WHERE length(province) > 1' | sed 's/[\t]/;/g'  >  c.csv 

# remover arquivo antigo do hdfs

echo 'Removendo arquivo /tmp/c.csv do HDFS'
hdfs dfs -rm /tmp/c.csv > /dev/null
 
# copiar novo arquivo para HDFS

hdfs dfs -put c.csv /tmp
echo 'Copiando novo arquivo CSV para o HDFS : /tmp/c.csv'

# rodar script do HBase

hbase shell < script_hbase.sql 

# rodar script de importacao de tabelas Hbase

echo 'Importando dados para o HBase'
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=';' -Dimporttsv.columns=HBASE_ROW_KEY,data:observationdate,data:province,data:country,data:lastupdate,data:confirmed,data:deaths,data:recovered covid /tmp/c.csv

