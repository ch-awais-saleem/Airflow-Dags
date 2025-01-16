from airflow import DAG
# from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.bash import BashOperator
from datetime import date

default_args = {
    'owner': 'Muhammad Awais Saleem',
    'start_date': date.now()
}


dag = DAG(
    'ML-1-DAG-for-scrapping',
    default_args=default_args,
    schedule_interval='11 11 * * *', 
    catchup=False
)

run_python_script = BashOperator(
    task_id='run_python_script',
    bash_command='python /home/awais/airflow/dags/hello.py',
    dag=dag,
)


python_script = BashOperator(
    task_id='python_script',
    bash_command='python /home/awais/airflow/dags/helloc.py',
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

# Set up dependencies here accordingly
# task_1 >> run_python_script >> task_3 >> python_script
 


