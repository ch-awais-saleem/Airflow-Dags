import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import imapclient
import pyzmail
import os


def download_attachments():
    # Connect to the Gmail IMAP server
    client = imapclient.IMAPClient('imap.gmail.com')

    # Login to your Gmail account
    client.login('awais.saleem@odysseyanalytics.net', 'Odyssey@123!')

    # Select the mailbox you want to read from
    client.select_folder('INBOX')

    # Search for the latest email
    messages = client.search()
    latest_msg_id = messages[-1]

    # Fetch the latest email
    email_data = client.fetch([latest_msg_id], ['RFC822'])[latest_msg_id]
    raw_email = email_data[b'RFC822']

    # Parse the email using pyzmail
    message = pyzmail.PyzMessage.factory(raw_email)

    # Iterate through the parts of the email
    for part in message.mailparts:
        if part.filename:
            # If the part has a filename, it is an attachment
            filename = part.filename.decode('utf-8')

            # Download the attachment to a local directory
            attachment_path = os.path.join(
                '/home/awais/airflow/dags/', filename)
            with open(attachment_path, 'wb') as f:
                f.write(part.get_payload())

    # Logout from the Gmail server
    client.logout()


default_args = {
    'owner': 'Muhammad Awais Saleem',
    'start_date': datetime.datetime(2023, 6, 12),
    'email': ['awais.saleem@odysseyanalytics.net'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG('gmail_attachment_downloader', default_args=default_args, schedule_interval='0 0 * * *') as dag:
    download_attachments_task = PythonOperator(
        task_id='download_attachments_task',
        python_callable=download_attachments
    )
