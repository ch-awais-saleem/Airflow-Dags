from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

# Define the default arguments for the DAG
default_args = {
    'owner': 'Xawais',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Define the functions to extract, transform, and load the data
def extract_data():
    # Your code to extract the data goes here
    response = requests.get('https://jsonplaceholder.typicode.com/todos')
    data = json.loads(response.text)
    df = pd.json_normalize(data)
    return df

def transform_data(df):
    # Your code to transform the data goes here
    df['completed'] = df['completed'].astype(int)
    df = df[['userId', 'id', 'title', 'completed']]
    return df

def load_data(df):
    # Your code to load the data goes here
    conn_id = 'my_postgres_connection'
    table_name = 'my_table'
    sql = f'INSERT INTO {table_name} (userId, id, title, completed) VALUES (%s, %s, %s, %s)'
    postgres = PostgresOperator(
        task_id='postgres',
        sql=sql,
        postgres_conn_id=conn_id,
        autocommit=True,
        parameters=df.values.tolist(),
        dag=dag,
    )
    return postgres

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    op_kwargs={'df': "{{ ti.xcom_pull(task_ids='extract_task') }}"},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    op_kwargs={'df': "{{ ti.xcom_pull(task_ids='transform_task') }}"},
    dag=dag,
)

# Define the dependencies between the tasks
transform_task.set_upstream(extract_task)
load_task.set_upstream(transform_task)
