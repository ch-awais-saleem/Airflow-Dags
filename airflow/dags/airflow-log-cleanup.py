import datetime
import os
from datetime import timedelta
import shutil
from datetime import datetime
import airflow
from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator



def delete_older_logs():
    dir_path = '/your/directory/path/' # Replace with the directory path
    days_threshold = 7 # The number of days after which the folders will be deleted

    now = datetime.now()

    for folder_name in os.listdir(dir_path):
        folder_path = os.path.join(dir_path, folder_name)
        if os.path.isdir(folder_path):
            modification_time = os.path.getmtime(folder_path)
            age = now - datetime.fromtimestamp(modification_time)
            if age > timedelta(days=days_threshold):
                shutil.rmtree(folder_path)

sending_email_notification = EmailOperator(
    task_id="sending_email",
    # depends_on_past=True,
    to="abc@gmail.net",
    subject="Logs Deletion",
    html_content="""
        <h3>Scheduler logs older than 7 days have been deleted.</h3>

    """
)


default_args = {
    'owner': 'Awais SALEEM',
    # 'depends_on_past': False,
    # 'email': ALERT_EMAIL_ADDRESSES,
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'Delete-log-periodically',
    default_args=default_args,
    start_date=datetime(2023, 4, 28),
    schedule_interval="06 10 * * *",
)


start = BashOperator(
    task_id='start',
    bash_command='echo "Starting log deletion"',
    dag=dag
)

delete = PythonOperator(
    task_id='delete_logs',
    python_callable=delete_older_logs,
    email='abc@gmail.net',
    email_on_failure =True,
    dag=dag

)

start >> delete >> sending_email_notification
