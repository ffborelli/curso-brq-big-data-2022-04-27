
-- criando banco de dados covid senao existir

create database if not exists covid;

use covid;

-- removendo a tabela caso ela exista
drop table if exists covid_19;

-- criando a tabela

CREATE TABLE `covid.covid_19`(
	  `sno` int, 
	  `observationdate` string, 
	  `province` string, 
	  `country` string, 
	  `lastupdate` string, 
	  `confirmed` int, 
	  `deaths` int, 
	  `recovered` int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)   ;

-- carregando os dados do CSV para a tabela Hive
LOAD data inpath '/user/cloudera/covid_19_data.csv' overwrite into table covid.covid_19;


