from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

def decide_branch(**context):
    ti = context['ti']
    xcom_value = ti.xcom_pull(task_ids='task_a')

    if xcom_value == 'condition_met':
        return 'task_b'
    else:
        return 'task_c'

default_args = {
    'owner': 'Awais Saleem',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'skipppp',
    default_args=default_args,
    schedule_interval='@once'
)

task_a = BashOperator(
    task_id='task_a',
    bash_command='echo "Condition met"',
    dag=dag
)

branching = BranchPythonOperator(
    task_id='branching',
    provide_context=True,
    python_callable=decide_branch,
    dag=dag
)

task_b = BashOperator(
    task_id='task_b',
    bash_command='echo "Task B completed successfully"',
    dag=dag
)

task_c = BashOperator(
    task_id='task_c',
    bash_command='echo "Task C completed successfully"',
    dag=dag
)

skip_task = BashOperator(
    task_id='skip_task',
    bash_command='echo "Task skipped"',
    dag=dag,
    trigger_rule='none_failed'
)

trigger_next_dag = TriggerDagRunOperator(
    task_id='trigger_next_dag',
    trigger_dag_id='example_dag_2',
    dag=dag,
    params={'message': 'Triggered from example_dag'}
)

task_a >> branching
branching >> task_b
branching >> task_c
task_b >> skip_task
task_c >> trigger_next_dag
