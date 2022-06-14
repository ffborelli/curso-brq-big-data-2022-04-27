from airflow.models import DAG
from airflow.operators.bash import BashOperator
#from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from subdags.subdag_parallel_dag import subdag_parallel_dag

from datetime import datetime

default_args = {
    'start_date' : datetime(2020,1,1)
}



with DAG('parallel_dag_3',schedule_interval='@daily', default_args=default_args , catchup=False,
    tags=['fgv']) as dag:
    #define tasks/operators
    
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        task_2 = BashOperator(
            task_id='task_2',
            bash_command='sleep 3'
        )

        task_3 = BashOperator(
            task_id='task_3',
            bash_command='sleep 3'
        )

    # processing = SubDagOperator(
    #     task_id='processing_tasks',
    #     subdag=subdag_parallel_dag('parallel_dag_2', 'processing_tasks', default_args)
    # )
        
    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )


    task_1 >> processing_tasks >> task_4

    