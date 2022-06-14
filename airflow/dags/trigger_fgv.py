from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag

from datetime import datetime

default_args = {
    'start_date' : datetime(2020,1,1)
}



with DAG('trigger_fgv',schedule_interval='@daily', default_args=default_args , catchup=False,
    tags=['fgv']) as dag:
    #define tasks/operators
    
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='exit 1',
        do_xcom_push=False
    )

    task_2 = BashOperator(
        task_id='task_2',
        bash_command='exit 1',
        do_xcom_push=False
    )

    task_3 = BashOperator(
        task_id='task_3',
        bash_command='exit 0',
        do_xcom_push=False,
        trigger_rule='all_failed'
    )

    [task_1,task_2] >> task_3




