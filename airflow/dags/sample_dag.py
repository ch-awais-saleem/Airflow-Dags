from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint


def _training_model():
    return randint(1, 10)


def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_id=[
        'model_A', 'model_B', 'model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


with DAG('best-executer',
         catchup=False,
         start_date=datetime(2023, 4, 1),
         schedule_interval='@hourly') as dag:

    training_model_A = PythonOperator(
        task_id='model_A',
        python_callable=_training_model
    )
    training_model_B = PythonOperator(
        task_id='model_B',
        python_callable=_training_model
    )
    training_model_C = PythonOperator(
        task_id='model_C',
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model

    )

    accurate = BashOperator(
        task_id='accurate',
        bash_command="echo Accurate"
    )

    inaccurate = BashOperator(
        task_id='inaccurate',
        bash_command="echo inaccurate"
    )


[training_model_A,training_model_B,training_model_C] >> choose_best_model >> [accurate,inaccurate]