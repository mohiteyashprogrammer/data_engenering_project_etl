import os
import sys
import boto3
import pandas as pd
import numpy as np
from dotenv import load_dotenv
load_dotenv()
from helpers.aws_simple_email_service import ErrorEmailSender
from helpers.exception import CustomException

email_sender = ErrorEmailSender(
    region_name=os.getenv("AWS_REGION_NAME"),
    source_email=os.getenv("SOURCE_EMAIL"),
    destination_email=os.getenv("DESTINATION_EMAIL"))


class AWS_S3Manager:
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
        try:
            if not bucket_name or not aws_access_key_id or not aws_secret_access_key or not region_name:
                raise ValueError("All AWS credentials and bucket name must be provided.")

            self.bucket_name = bucket_name
            self.s3_client = boto3.client(
                service_name='s3',
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print("Connected to AWS S3 bucket")
        except self.s3_client.exceptions.NoSuchBucket:
            raise CustomException(f"The bucket '{bucket_name}' does not exist.", sys)
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error Occurred while connecting to AWS S3: {e}"
            )
            raise CustomException(e, sys)

    def upload_file(self, local_filename, s3_key):
        """
        Upload file to S3 bucket.
        Args:
            local_filename (str): Local file path.
            s3_key (str): S3 object key.
        """
        try:
            if not os.path.exists(local_filename):
                raise FileNotFoundError(f"The file '{local_filename}' does not exist.")
            if not s3_key:
                raise ValueError("S3 key cannot be empty.")

            self.s3_client.upload_file(local_filename, self.bucket_name, s3_key)
            print(f"File '{local_filename}' uploaded to S3 bucket as '{s3_key}'.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while uploading file '{local_filename}' to S3 bucket: {e}"
            )
            raise CustomException(e, sys)

    def upload_dataframe_to_s3(self, df, s3_key):
        """
        Uploads a Pandas DataFrame to the S3 bucket with the specified key.
        Args:
            df (pd.DataFrame): The DataFrame to upload.
            s3_key (str): The S3 key (object key) under which to store the DataFrame.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise ValueError("The provided object is not a valid Pandas DataFrame.")
            if df.empty:
                raise ValueError("The DataFrame is empty and cannot be uploaded.")
            if not s3_key:
                raise ValueError("S3 key cannot be empty.")

            # Convert DataFrame to CSV format
            csv_buffer = df.to_csv(index=False).encode('utf-8')

            # Upload the CSV data to S3
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=csv_buffer)
            print(f"DataFrame uploaded to S3 bucket as '{s3_key}'.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error uploading DataFrame: {str(e)}"
            )
            raise CustomException(e, sys)

    def download_file(self, s3_key, local_filename, target_directory="data"):
        """
        Download file from S3 bucket.
        Args:
            s3_key (str): S3 object key.
            local_filename (str): Local file name to save as.
            target_directory (str): Directory to save the file.
        """
        try:
            if not s3_key:
                raise ValueError("S3 key cannot be empty.")
            if not local_filename:
                raise ValueError("Local filename cannot be empty.")
            
            if not os.path.exists(target_directory):
                os.makedirs(target_directory)  # Create the target directory if it doesn't exist

            # Construct the full local path
            local_path = os.path.join(target_directory, local_filename)

            # Check if the object exists in S3
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)

            # Download the file
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"File '{s3_key}' downloaded from S3 bucket and saved as '{local_path}'.")
        except self.s3_client.exceptions.NoSuchKey:
            raise CustomException(f"The key '{s3_key}' does not exist in the bucket.", sys)
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while downloading file from S3 bucket: {e}"
            )
            raise CustomException(e, sys)

    def read_csv_from_s3(self, s3_key):
        """
        Reads CSV file from S3 bucket.
        Args:
            s3_key (str): S3 object key.
        Returns:
            pd.DataFrame: DataFrame containing the data.
        """
        try:
            if not s3_key:
                raise ValueError("S3 key cannot be empty.")
            
            # Check if the object exists in S3
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)

            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            df = pd.read_csv(obj['Body'])
            return df
        except self.s3_client.exceptions.NoSuchKey:
            raise CustomException(f"The key '{s3_key}' does not exist in the bucket.", sys)
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while reading data from S3 bucket: {e}"
            )
            raise CustomException(e, sys)

    def move_data(self, source_folder, destination_folder):
        """
        Moves data in S3 bucket from source_folder to destination_folder.
        Args:
            source_folder (str): Source folder path in S3.
            destination_folder (str): Destination folder path in S3.
        """
        try:
            if not source_folder or not destination_folder:
                raise ValueError("Source and destination folders cannot be empty.")

            # List objects in the source folder
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=source_folder)
            if 'Contents' in response:
                for obj in response['Contents']:
                    if not obj['Key'].endswith('/'):  # Ignore folders
                        # Construct the new key destination path
                        new_key = obj['Key'].replace(source_folder, destination_folder, 1)
                        # Copy the object to the new location
                        self.s3_client.copy_object(
                            Bucket=self.bucket_name,
                            CopySource={'Bucket': self.bucket_name, 'Key': obj['Key']},
                            Key=new_key
                        )
                        # Delete the original object
                        self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
                        print(f"Moved {obj['Key']} to {new_key}")
            else:
                print(f"No objects found in {source_folder}.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while moving data: {str(e)}"
            )
            raise CustomException(e, sys)