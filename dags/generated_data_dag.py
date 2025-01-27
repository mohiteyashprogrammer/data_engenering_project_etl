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
from helpers.aws_s3_manager import AWS_S3Manager
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync
from helpers.aws_simple_email_service import ErrorEmailSender
from helpers.exception import CustomException
from dotenv import load_dotenv
load_dotenv()

email_sender = ErrorEmailSender(
    region_name=os.getenv("AWS_REGION_NAME"),
    source_email=os.getenv("SOURCE_EMAIL"),
    destination_email=os.getenv("DESTINATION_EMAIL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_ID"))
    
    
import csv
import random
import string
import time
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def generate_csv_files_to_s3(
    num_rows, 
    num_files,
    bucket_name, 
    s3_folder, 
    aws_access_key=None,
    aws_secret_key=None,
    wait_time=10):
    """
    Generate CSV files and upload them to an S3 bucket, continuing the file naming sequence.
    """
    try:
        # Define columns
        columns = [
            "exported", "productid", "product", "price", "depacher", "arrival",
            "dateofcreation", "location", "age", "group", "usage", "product_age"
        ]
        
        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key, 
            aws_secret_access_key=aws_secret_key,
        )
        
        # Ensure the folder name ends with a '/'
        if not s3_folder.endswith('/'):
            s3_folder += '/'
        
        # List existing files in the S3 folder
        existing_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)
        print("Existing files in S3 folder:", existing_files)  # Debug statement
        
        file_numbers = set()
        
        if 'Contents' in existing_files:
            # Extract file numbers from existing files
            for file in existing_files['Contents']:
                file_name = file['Key'].split('/')[-1]  # Get the file name without the path
                print("Processing file:", file_name)  # Debug statement
                
                # Skip folder-like objects (e.g., objects with a trailing '/')
                if not file_name:
                    print("Skipping folder-like object:", file['Key'])
                    continue
                
                if file_name.startswith("data_file_") and file_name.endswith(".csv"):
                    try:
                        file_number = int(file_name.replace("data_file_", "").replace(".csv", ""))
                        print("Extracted file number:", file_number)  # Debug statement
                        file_numbers.add(file_number)
                    except ValueError:
                        print(f"Skipping file with invalid number: {file_name}")  # Debug statement
                        continue

        # Find the next available file number
        starting_file_number = 1
        while starting_file_number in file_numbers:
            starting_file_number += 1

        print("Starting file number:", starting_file_number)  # Debug statement

        for file_counter in range(starting_file_number, starting_file_number + num_files):
            file_name = f"data_file_{file_counter}.csv"
            rows = []

            # Generate rows
            for _ in range(num_rows):
                row = {
                    "exported": random.choice(["Yes", "No"]),
                    "productid": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
                    "product": f"Product-{random.randint(1, 500)}",
                    "price": round(random.uniform(10.0, 1000.0), 2),
                    "depacher": f"Dep-{random.randint(1, 50)}",
                    "arrival": f"Arr-{random.randint(1, 50)}",
                    "dateofcreation": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "location": random.choice(["Mumbai", "Delhi", "Bangalore", "Pune"]),
                    "age": random.randint(18, 60),
                    "group": random.choice(["A", "B", "C", "D"]),
                    "usage": random.choice(["Low", "Medium", "High"]),
                    "product_age": random.randint(1, 10),
                }
                rows.append(row)

            # Write to CSV
            with open(file_name, mode="w", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)

            print(f"Generated file: {file_name}")

            # Upload to S3
            s3_key = f"{s3_folder}{file_name}"
            try:
                s3_client.upload_file(file_name, bucket_name, s3_key)
                print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
            except Exception as e:
                print(f"Failed to upload {file_name} to S3: {e}")
                raise

            # Delete local file
            os.remove(file_name)
            print(f"Deleted local file: {file_name}")

            # Wait before next file
            if file_counter < starting_file_number + num_files - 1:
                print(f"Waiting {wait_time} seconds before generating the next file...")
                time.sleep(wait_time)

    except Exception as e:
        print(f"An error occurred: {e}")
        # email_sender.send_error_email(f"An error occurred: {e}")  # Uncomment if using email notifications
        raise Exception(e)


def generate_data():
    """
    Generate data using the generate_csv_files_to_s3 function.
    """
    
    aws_cred = Variable.get("s3_bucket", deserialize_json=True)
    aws_bucket_name = aws_cred["bucket_name"]
    aws_access_key_id = aws_cred["access_key_id"]
    aws_secret_access_key = aws_cred["secret_access_key"]
    print(f"Bucket Name: {aws_bucket_name}")
    FOLDER_NAME = "generated_data"
    try:
        generate_csv_files_to_s3(
            num_rows=1000,  
            num_files=1, 
            bucket_name= aws_bucket_name,
            s3_folder=FOLDER_NAME,
            aws_access_key=aws_access_key_id,
            aws_secret_key=aws_secret_access_key,
            wait_time=20
        )
    except CustomException as e:
        email_sender.send_error_email(e)
    

# Define the default_args dictionary
default_args = {
    'owner': 'yashmohite',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='Data_Generation_DAG',
    default_args=default_args,
    description="A DAG to generate data and upload it to an S3 bucket.",
    start_date=datetime(2024, 7, 1),
    schedule_interval='0 4 * * 1-6',  #  schedule a task to run at 9:30 AM IST (Indian Standard Time) every Monday to Saturday.
    catchup=False
) as dag:
    
    
    # Task to generate data
    generate_data_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        dag=dag
    )
    
    generate_data_task
    