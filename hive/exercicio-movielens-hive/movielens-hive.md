### Exercício Hive


Importar os dados dos arquivos: movies.t, occupation.dat, ratings.t e users.t

Segue script para a criação das tabelas no Hive:

```

create database movielens
use movielens

CREATE TABLE ratings (
  userid INT, 
  movieid INT,
  rating INT, 
  tstamp STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
STORED AS TEXTFILE;


CREATE TABLE movies (
  movieid INT, 
  title STRING,
  genres ARRAY<STRING>
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
COLLECTION ITEMS TERMINATED BY "|"
STORED AS TEXTFILE;

CREATE TABLE users (
  userid INT, 
  gender STRING, 
  age INT,
  occupation INT,
  zipcode STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
STORED AS TEXTFILE;

CREATE TABLE occupations (
  id INT,
  occupation STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '#'
STORED AS TEXTFILE;

```

# Perguntas

- filmes com nota 5
- quais as ocupações (profissões) com filmes com nota 5
- mostrar a relação entre gênero e rating (notas)
- Nota do filme de acordo com a idade