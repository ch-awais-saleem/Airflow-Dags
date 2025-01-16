import os
import sqlalchemy as sa
import psycopg2
from psycopg2 import extensions as _ext
from sqlalchemy import engine as sa_url
import re
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
# from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator



def db_init():
    try:

        # Get the database credentials from the Airflow Connection
        conn_id = ""  # Replace with your Airflow Connection ID 
        airflow_var = Variable.get(conn_id, deserialize_json=True)

        # # Create the connection URL
        engine = sa_url.URL.create(
            drivername=airflow_var['drivername'],
            username=airflow_var['username'],
            password=airflow_var['password'],
            host=airflow_var['host'],
            port=airflow_var['port'],
            database=airflow_var['database']
        )

        engine = sa.create_engine(engine, execution_options={"quote_char": ''})
   
        conn2 = psycopg2.connect(
            host=airflow_var['host'],
            database=airflow_var['database'],
            user=airflow_var['username'],
            password=airflow_var['password'],
            port=airflow_var['port']
         )
        
        return conn2,engine
    except Exception as e:
        error_message = str(e)
        create_log_error(error_message, log_error=True)
        raise 
	    # Re-raise the exception to let Airflow handle the failure



def dataprocessing():
    task_name='alkuraimi-kedwh-migration'
    execution_datetime = datetime.now()
    try:
        print("Function Execution started at: ")
        conn2,engine=db_init()
        table_name = ""
        config_data = pd.read_sql(f"SELECT * FROM {table_name} ", engine)
        print('Psycopg2 Cursor')
        cursor = conn2.cursor()
        #table_list = ['']
        
        for i,row in config_data.iterrows():

            if row[2] in table_list:

                # """ Instead of row[0]   result config table['column_name']  can be used """
                schema_mssql = row[0]
                target_tbl = row[1]
                staging_table_name = row[2]
                pk_columns = row[3]
                delete_query = row[4]
                del_update_staging = row[5]
                update_insert_query = row[6]
                update_delete_query = row[7]
                update_staging = row[8]
                insert_query = row[9]
                insert_update_staging = row[10]
                print(target_tbl)

                cursor.execute(f""" DROP table  IF EXISTS temp_staging """)
                cursor.execute(f"""
                    CREATE temporary TABLE temp_staging AS
                    SELECT * FROM dbo.{staging_table_name} WHERE isprocessed = False AND DATE(updated_date_spark) = (CURRENT_DATE)
                """)
                print("Temporary table created successfully")

                # Fetch the rows from the temporary table
                cursor.execute("SELECT * FROM temp_staging")
                rows = cursor.fetchall()
                try:
                    # print("Temporary table created successfully")
                    if len(delete_query) > 0:
                        cursor.execute(f"{delete_query}; ")

                        print("deletion completed");

                    if len(del_update_staging) > 0:
                        cursor.execute(f"{del_update_staging} ;")
                        conn2.commit()
                        # print("update staging after deletion operation -- completed")
                except Exception as e:
                    error_message = str(e)
                    raise Exception(error_message)
                    # send_error_email(e)
	            
                try:    
                    if len(update_delete_query) > 0:
                        cursor.execute(f"{update_delete_query} ")
                        print("FOr update operation deletion -- completed")
                        # print(update_delete_query)
                    if len (update_insert_query) > 0:
                        cursor.execute(f"{update_insert_query} ;")
                        print("For update operation Insert step -- completed")
                        # print(update_staging)
                        cursor.execute(f"{update_staging}; ")
                        print("update staging after update operation  -- completed")
                        conn2.commit()
                except Exception as e:
                    error_message = str(e)
                    raise Exception(error_message)
                    # send_error_email(e)
	            
                try:
		    # print the insertion query below
                    # print(insert_query)
                    # print(insert_update_staging)
                    if len(insert_query) > 0:
                        cursor.execute(f"{insert_query} ;")
                        print("Insertion completed")
                    if len(insert_update_staging) > 0:    
                        cursor.execute(f"{insert_update_staging}; ")
                        print("update staging after Insertion operation -- completed")
                        conn2.commit()
                except Exception as e:
                    error_message = str(e)
                    raise Exception(error_message)
                    # send_error_email(e)
		
                finally:
                    cursor.execute("DROP table  IF EXISTS temp_staging")
                #     # rows=cursor.fetchall()

                #     # # # Print the rows

                #     # for row in rows:
                #     #    print(row)
        cursor.close()

    except Exception as e:
        error_message = str(e)
        create_log_error(error_message, log_error=True)
        conn2,cursor=db_init()
        insert_query = f"""
            INSERT INTO dbo.process_logs (task_name, log_message, execution_datetime, log_type) 
            VALUES ('{task_name}', '{error_message}', '{execution_datetime}', 'error')
        """
        cursor.execute(insert_query)
        # cursor.close()
        conn2.commit()
        # conn2.close()
        raise  # Re-raise the exception to let Airflow handle the failure
    else:
        # If no exception occurs, log the informational message
        create_log_success()
        conn2,cursor=db_init()
        insert_query = f"""
            INSERT INTO dbo.process_logs (task_name, log_message, execution_datetime, log_type) 
            VALUES ('{task_name}', 'Task Executed Successfully.', '{execution_datetime}', 'success')
        """
        cursor.execute(insert_query)
        # cursor.close()
        conn2.commit()
        # conn2.close()


start = EmptyOperator(
    task_id='start'
)

log_folder = "/root/airflow/kedwh-logs/alkuraimi-kedwh-migration/"


def create_log_error(log_message, log_error=False):
    # Get the current date and time
    current_datetime = datetime.now()
    date_string = current_datetime.strftime("%Y-%m-%d")

    # Create the log folder if it doesn't exist
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Generate the log file path to check logs in detail later
    log_file_path = os.path.join(log_folder, f"{date_string}_log.txt")

    # Write the log message to the log file
    with open(log_file_path, 'a') as file:
        if log_error:
            file.write(f"ERROR [{current_datetime}]: {log_message}\n")
            file.write(f"ERROR [{current_datetime}]: {error}\n")
        else:
            file.write(f"")


def create_log_success():
    # Get the current date and time
    current_datetime = datetime.now()
    date_string = current_datetime.strftime("%Y-%m-%d")

    # Create the log folder if it doesn't exist
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Generate the log file path
    log_file_path = os.path.join(log_folder, f"{date_string}_log.txt")

    # Write the log message to the log file
    with open(log_file_path, 'a') as file:
        file.write(f"INFO  [{current_datetime}]: {success}\n")


# Example usage:
success = "[Airflow] Task Succeeded."
error = "[Airflow] Task Failed."

end = EmptyOperator(
    task_id='end'
)


dag1 = DAG(
    dag_id='alkuraimi-kedwh-migration',
    description='Ke Dwh Data Processing',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 8, 20),
    catchup=False,
    default_args={
        # 'on_failure_callback': create_log_error(error),
        'owner': 'M Awais Saleem'
    }
)

stagingdata = PythonOperator(
    task_id='reading_staging_tables',
    python_callable=dataprocessing,
    provide_context=True,
    dag=dag1
)


start >> stagingdata >> end
