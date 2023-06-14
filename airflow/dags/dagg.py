from airflow import DAG
# from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Muhammad Awais Saleem',
    'start_date': datetime(2023, 3, 22)
}


dag = DAG(
    'python-file-dag',
    # start_date=datetime(2023, 3, 21),
    default_args=default_args,
    schedule_interval='26 08 * * *',  # run every day at midnight
    tags=["Sample", "Scrapper-scheduled"],
    catchup=False
)

run_python_script = BashOperator(
    task_id='run_python_script',
    # bash_command='python /home/awais/airflow/dags/stg_dow_jones_industrial_average.py',
        bash_command='python /home/awais/airflow/dags/hello.py',

    dag=dag,
)


task_1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Executing scrape file"',
    dag=dag
)

task_3 = BashOperator(
    task_id='task_3',
    bash_command='echo "Executed Task"',
    dag=dag
)

# Set up dependencies
# task_1 >> run_python_script >> task_3
