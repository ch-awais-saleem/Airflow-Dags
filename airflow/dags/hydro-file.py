import shutil
from airflow.sensors.filesystem import FileSensor
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import glob
import boto3
from airflow.operators.python import PythonOperator
import os
from airflow.models import Variable



# Set the AWS access key ID and secret access key
aws_access_key_id = Variable.get("aws_access_key_id_secret")
aws_secret_access_key = Variable.get("aws_secret_access_key")

# Set the AWS region
region_name = 'us-east-1'

# Create an S3 client
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key, region_name=region_name)


def transfer_latest_file_to_s3():
    # Set the folder path
    folder_path = '/home/awais/airflow/bin/'

    # Get the list of files in the folder
    file_list = glob.glob(os.path.join(folder_path, '*'))

    # Sort the files by modification time (latest first)
    file_list.sort(key=os.path.getmtime, reverse=True)

    # Get the latest file
    if len(file_list) > 0:
        latest_file_path = file_list[0]

        # Set the S3 destination details
        s3_bucket_name = 'odyssey-data-lake'
        s3_key = 'hydro-carbon/' + os.path.basename(latest_file_path)

        # Upload the latest file to S3
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            latest_file_path,
            s3_bucket_name,
            s3_key
        )
        print("file --- name ="+os.path.basename(latest_file_path))
        global source_file
        source_file = "/home/awais/airflow/bin/" + os.path.basename(latest_file_path)+''
        current_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        global destination_file
        destination_file = "/home/awais/airflow/processed_files/Processed_"+current_time +'_'+  os.path.basename(latest_file_path)+''
        



default_args = {
    'owner': 'Muhammad Awais Saleem',
    'start_date': datetime(2023, 6, 13)
}


dag = DAG(
    'wait_for_file_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2023, 6, 12)
)



start = EmptyOperator(
    task_id='start'
)

file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    poke_interval=20,
    timeout=600,
    mode='poke',
    filepath='/home/awais/airflow/bin/',
    dag=dag
)



upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=transfer_latest_file_to_s3

)


def mark_processed():
    shutil.move(source_file, destination_file)

move_to_procesed_folder = PythonOperator(
    task_id="move_to_procesed_folder",
    python_callable=mark_processed

)








end = EmptyOperator(
    task_id='end'
)

start >> file_sensor_task >> upload_to_s3 >> move_to_procesed_folder >> end
