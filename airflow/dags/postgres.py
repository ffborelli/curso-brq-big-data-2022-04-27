import uuid
from datetime import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.postgres_operator import PostgresOperator


# dag_params = {
#     'dag_id': 'postgres',
#     'start_date': datetime(2019, 10, 7),
#     'schedule_interval': None
# }

default_args = {
    'start_date' : datetime(2020,1,1),
     'schedule_interval': None
}


with DAG('postgres', schedule_interval='@daily', default_args=default_args, catchup=False,
    tags=['fgv']) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE new_table(
            custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
            );''',
    )

    insert_row = PostgresOperator(
        task_id='insert_row',
        sql='INSERT INTO new_table VALUES(%s, %s, %s)',
        trigger_rule=TriggerRule.ALL_DONE,
        parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
    )

    create_table >> insert_row