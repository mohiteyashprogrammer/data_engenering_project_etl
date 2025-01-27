import csv
import random
import string
import time
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import os
import sys
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from plugins.helpers.postgray_manager import PostgresConnector
from helpers.product_data_transformation import process_data
from helpers.aws_s3_manager import AWS_S3Manager
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync
from helpers.aws_simple_email_service import ErrorEmailSender
from helpers.exception import CustomException
from plugins.helpers.configure import CREATE_PRODUCT_TABLE
from dotenv import load_dotenv
load_dotenv()

email_sender = ErrorEmailSender(
    region_name=os.getenv("AWS_REGION_NAME"),
    source_email=os.getenv("SOURCE_EMAIL"),
    destination_email=os.getenv("DESTINATION_EMAIL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_ID"))


def get_sql_query(file_path: str) -> str:
        """
        Reads a SQL file and fetches the query back as a Python string.

        Args:
            file_path (str): SQL file path to be read.

        Returns:
            str: SQL query for a particular task.
        """
        try:
            with open(file_path, 'r') as file:
                return file.read()
        except FileNotFoundError as e:
            print("ERROR::", e)
            raise e

def save_dataframe_to_s3(
    df, 
    bucket_name="your-s3-bucket", 
    aws_access_key=None,
    aws_secret_key=None,
    folder="transform_json_de_products_data",
):
    """
    Save the provided DataFrame directly as JSON to S3.
    The file name is dynamically determined by the existing files in the S3 folder.
    """
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key, 
        aws_secret_access_key=aws_secret_key,
    )
    
    # Get the existing files in the specified S3 folder
    existing_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder).get('Contents', [])
    
    # If the folder is empty, existing_files will be empty, handle this case
    if existing_files:
        # Extract file names to count how many files already exist
        existing_files = [f['Key'] for f in existing_files]
    else:
        existing_files = []

    # Determine the next file number (e.g., data_file1.json, data_file2.json, ...)
    file_number = len(existing_files) + 1
    json_filename = f"{folder}/data_file{file_number}.json"

    # Convert the DataFrame to JSON string (in records format, one line per record)
    json_data = df.to_json(orient="records", lines=True)

    # Upload the JSON string directly to S3
    s3_client.put_object(Bucket=bucket_name, Key=json_filename, Body=json_data)
    print(f"Data saved to S3 bucket '{bucket_name}' at location '{json_filename}'")

    return json_filename


def data_transformation(**kwargs):
    try:
        # Get the AWS S3 credentials from Airflow Variables
        aws_cred = Variable.get("s3_bucket", deserialize_json=True)
        aws_bucket_name = aws_cred["bucket_name"]
        aws_access_key_id = aws_cred["access_key_id"]
        aws_secret_access_key = aws_cred["secret_access_key"]
        region_name = aws_cred["region_name"]
        
        # Initialize AWS S3 manager
        aws_manager = AWS_S3Manager(
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            bucket_name=aws_bucket_name
        )
        
        # Read the CSV file from S3
        df = aws_manager.read_csv_from_s3(
        s3_key="generated_data/data_file_1.csv"
    )
        # Process the data
        transform_data = process_data(df)
        
        kwargs['ti'].xcom_push(key='transform_data', value=transform_data)
        
        
    except Exception as e:
        # Send an email notification when an error occurs
        email_sender.send_error_email(
                message_to_send=f"Error during data transformation: {str(e)}",
            )
        raise CustomException(f"Error during data transformation: {str(e)}")
    
    
    
def save_transformed_data_to_s3_json(**kwargs):
    
    try:
        # Get the AWS S3 credentials from Airflow Variables
        aws_cred = Variable.get("s3_bucket", deserialize_json=True)
        aws_bucket_name = aws_cred["bucket_name"]
        aws_access_key_id = aws_cred["access_key_id"]
        aws_secret_access_key = aws_cred["secret_access_key"]
        
        # Get the transformed data from XCom
        df = kwargs['ti'].xcom_pull(key='transform_data',task_ids='product_data_transformation')
        
        json_file = save_dataframe_to_s3(
            df=df,
            bucket_name=aws_bucket_name,
            aws_access_key=aws_access_key_id,
            aws_secret_key=aws_secret_access_key,
            folder="transform_json_de_products"
        )
    
    except Exception as e:
        # Send an email notification when an error occurs
        email_sender.send_error_email(
                message_to_send=f"Error during saving transformed data to S3: {str(e)}",
            )
        raise CustomException(f"Error during saving transformed data to S3: {str(e)}")
    
    
    
def copy_data_to_postgres_table(**kwargs):
    try:
        
        # Get the transformed data from XCom
        df = kwargs['ti'].xcom_pull(key='transform_data',task_ids='product_data_transformation')
        
        # Get the Postgres credentials from Airflow Variables
        postgres_cred = Variable.get("postgre_connection", deserialize_json=True)
        
        CRED = {
            'database': postgres_cred['database'],
            'user': postgres_cred['user'],
            'password': postgres_cred['password'],
            'host': postgres_cred['host'],
            'port': postgres_cred['port']
        }
        
        pg_conn = PostgresConnector(CRED)
        
        pg_conn.execute_cr_table_query(get_sql_query(CREATE_PRODUCT_TABLE))
        
        pg_conn.copy_from_dataframe(df, 'ym_de_products', 'product_table')
        
    except Exception as e:
        # Send an email notification when an error occurs
        email_sender.send_error_email(
                message_to_send=f"Error during copying data to Postgres table: {str(e)}",
            )
        raise CustomException(f"Error during copying data to Postgres table: {str(e)}")
    
    
def move_files_to_archive():
    try:
        # Get the AWS S3 credentials from Airflow Variables
        aws_cred = Variable.get("s3_bucket", deserialize_json=True)
        aws_bucket_name = aws_cred["bucket_name"]
        aws_access_key_id = aws_cred["access_key_id"]
        aws_secret_access_key = aws_cred["secret_access_key"]
        
        # Initialize AWS S3 manager
        aws_manager = AWS_S3Manager(
            region_name=aws_cred["region_name"],
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            bucket_name=aws_bucket_name
        )
        
        # Move the generated data files to the archive folder
        aws_manager.move_data(
        source_folder="generated_data/data_file_1.csv", 
        destination_folder="generated_data_archive_files/"
        )
        
    except Exception as e:
        # Send an email notification when an error occurs
        email_sender.send_error_email(
                message_to_send=f"Error during moving files to archive: {str(e)}",
            )
        raise CustomException(f"Error during moving files to archive: {str(e)}")
        

# Define the default_args dictionary
default_args = {
    'owner': 'yashmohite',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

AWS_CONN_ID = "aws_conn_id"
DATA_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
POKE_INTERVAL = 1 * 60 # poke interval set to 1minutes


# Define the DAG
with DAG(
    dag_id='Product_DAG',
    default_args=default_args,
    description="Product ETL process DAG",
    start_date=datetime(2024, 7, 1),
    schedule_interval='0 4 * * 1-6',  #  schedule a task to run at 9:30 AM IST (Indian Standard Time) every Monday to Saturday.
    catchup=False
) as dag:
    
    
     # sensor task to check for the file in the S3 bucket
    wait_for_file = S3KeySensorAsync(
        task_id="wait_for_file",
        bucket_key=f"s3://{DATA_BUCKET_NAME}/generated_data/*.csv",
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,
        poke_interval=POKE_INTERVAL,
        timeout=24 * 60 * 60  # 24 hours in seconds
    )
    
    
    data_transformation_task = PythonOperator(
        task_id='product_data_transformation',
        python_callable=data_transformation,
        provide_context=True,
        dag=dag
    )
    
    with TaskGroup(group_id='data_dumping',default_args={"retries": 2,'retry_delay': timedelta(seconds=10)}) as data_dumping:
    
        dump_transformation_to_s3 = PythonOperator(
            task_id='save_transformed_data_to_s3',
            python_callable=save_transformed_data_to_s3_json,
            provide_context=True,
            dag=dag
            
        )
        
        copy_to_postgres = PythonOperator(
            task_id='copy_data_to_postgres',
            python_callable=copy_data_to_postgres_table,
            provide_context=True,
            dag=dag
            
        )
    
    move_to_archive = PythonOperator(
        task_id='move_files_to_archive',
        python_callable=move_files_to_archive,
        provide_context=True,
        dag=dag
        
    )
    
    
    wait_for_file >> data_transformation_task >> data_dumping >> move_to_archive
    



