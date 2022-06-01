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


# Passos 

- copiar os arquivos para o HDFS

```
  hdfs dfs -mkdir /user/cloudera/movielens

  hdfs dfs -put *.dat /user/cloudera/movielens
  hdfs dfs -put *.t /user/cloudera/movielens
```

- criar as tabelas no Hive

- Maperar os dados que estão no HDFS com as tabelas do passo anterior



```

  LOAD DATA INPATH '/user/cloudera/movielens/movies.t' 
      OVERWRITE INTO TABLE movies;

  LOAD DATA INPATH '/user/cloudera/movielens/users.t' 
      OVERWRITE INTO TABLE users;

  LOAD DATA INPATH '/user/cloudera/movielens/ratings.t' 
      OVERWRITE INTO TABLE ratings;

  LOAD DATA INPATH '/user/cloudera/movielens/occupation.dat' 
      OVERWRITE INTO TABLE occupations;

```

- filmes com nota 5

```
SELECT * FROM movielens.movies m 
    INNER JOIN movielens.ratings r
        ON (r.movieid = m.movieid) 
    WHERE   
        r.rating = 5;
```

- quais as ocupações (profissões) com filmes com nota 5

```
SELECT distinct (o.occupation) FROM movielens.movies m 
    INNER JOIN movielens.ratings r
        ON (r.movieid = m.movieid) 
    INNER JOIN movielens.users u 
        ON (u.userid = r.userid)
    INNER JOIN movielens.occupations o
        ON (o.id = u.occupation)
    WHERE   
        r.rating = 5;

```

- mostrar a relação entre gênero e rating (notas)

```
  SELECT avg(r.rating) AS Media,
        gender,
        count(r.rating) AS Avaliacoes
  FROM ratings r
  JOIN users u ON r.userid = u.userid
  GROUP BY gender
  ORDER BY Avaliacoes;
```


```
select 
      a.gender generos, 
      b.rating notas, 
      count(b.rating) qtd, 
      round(100*count(rating)/1000209) percentual
  from users a
    inner join ratings b 
      on a.userid = b.userid
  group by a.gender, b.rating
```



