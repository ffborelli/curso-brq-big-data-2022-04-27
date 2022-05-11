
import mysql.connector
db = mysql.connector.connect(host='mysql', user='root', \
    password='root', port=3306)

my_cursor=db.cursor()
my_cursor.execute('DROP database IF EXISTS brq')
my_cursor.execute('CREATE DATABASE IF NOT EXISTS brq_python')
my_cursor.execute('use brq_python')
my_cursor.execute('CREATE TABLE IF NOT EXISTS feras_brq( nome varchar(255), email varchar(255) )')
my_cursor.execute('INSERT INTO feras_brq (nome,email) VALUES ("Joao", "j@j.com" )')

db.commit()

my_cursor.close()
