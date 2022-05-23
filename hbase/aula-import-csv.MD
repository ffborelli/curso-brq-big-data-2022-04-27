# Importação arquivo CSV para HBase

## Criar um arquivo com o nome: employees.csv e colocar os dados abaixo:

```
1,Lucy,Engineering
2,Milton,Engineering
3,Edith,Support
```

Entrar no HBase shell (hbase shell) e criar a tabela employees:

```
hbase shell
create 'employees', 'name', 'department'
```

Carregar employees.csv para dentro do HDFS.

```
hdfs dfs -put employees.csv /tmp
```

Usar ImportTsv para carregar os dados do HDFS (/tmp/employees.csv) dentro da tabela
  employees criada no passo anterior.

```
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=',' -Dimporttsv.columns=HBASE_ROW_KEY,name,department employees /tmp/employees.csv
```

Voltar para o HBase shell e verificar se os dados foram importador com sucesso:

```
hbase shell
scan 'employees'
```
