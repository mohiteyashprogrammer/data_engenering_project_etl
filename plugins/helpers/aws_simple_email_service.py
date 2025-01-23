import os
import sys
import boto3
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from botocore.exceptions import BotoCoreError, ClientError
from helpers.exception import CustomException

class ErrorEmailSender:
    def __init__(self, region_name, source_email, destination_email):
        try:
            if not region_name or not isinstance(region_name, str):
                raise ValueError("A valid AWS region name must be provided.")

            if not source_email or not isinstance(source_email, str):
                raise ValueError("A valid source email address must be provided.")

            if not destination_email or not isinstance(destination_email, str):
                raise ValueError("A valid destination email address must be provided.")

            self.session = boto3.Session(region_name=region_name)
            self.client = self.session.client("ses")
            self.source_email = source_email
            self.destination_email = destination_email
        except Exception as e:
            print(f"Error initializing ErrorEmailSender: {e}")
            raise CustomException(e, sys)

    def send_error_email(self, message_to_send, attachment=None, log_folder=None):
        '''
        This method sends an error email to the specified email address.

        Args:
            message_to_send (str): The error message to include in the email.
            attachment (list, optional): List of file paths to attach as files. Default is None.
            log_folder (str, optional): Path to the log folder for your specific DAG. Default is None.
        '''
        try:
            if not message_to_send or not isinstance(message_to_send, str):
                raise ValueError("A valid error message string must be provided.")

            msg = MIMEMultipart()
            msg["Subject"] = "Error Occurred!"
            msg["From"] = self.source_email
            msg["To"] = self.destination_email

            # Attach the error message body
            body = MIMEText(message_to_send)
            msg.attach(body)

            # Handle attachment if provided
            if attachment:
                if isinstance(attachment, pd.DataFrame):
                    if attachment.empty:
                        raise ValueError("The provided DataFrame is empty and cannot be attached.")

                    csv_data = attachment.to_csv(index=False)
                    part = MIMEApplication(csv_data, _subtype="csv")
                    part.add_header("Content-Disposition", "attachment", filename="data.csv")
                    msg.attach(part)
                elif isinstance(attachment, str):
                    if not os.path.exists(attachment):
                        raise FileNotFoundError(f"The file '{attachment}' does not exist.")

                    with open(attachment, "rb") as f:
                        attachment_data = f.read()
                    part = MIMEApplication(attachment_data)
                    part.add_header("Content-Disposition", "attachment", filename=os.path.basename(attachment))
                    msg.attach(part)
                else:
                    raise ValueError("Invalid attachment type. Provide a DataFrame or a valid file path.")

            # Handle log folder if provided
            if log_folder:
                if not os.path.exists(log_folder):
                    raise FileNotFoundError(f"The log folder '{log_folder}' does not exist.")

                for root, _, files in os.walk(log_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        with open(file_path, "rb") as f:
                            attachment_data = f.read()
                        part = MIMEApplication(attachment_data)
                        part.add_header("Content-Disposition", "attachment", filename=os.path.basename(file_path))
                        msg.attach(part)

            # Attempt to send the email
            response = self.client.send_raw_email(
                Source=self.source_email,
                Destinations=[self.destination_email],
                RawMessage={"Data": msg.as_string()},
            )
            print(f"Email sent successfully. Message ID: {response['MessageId']}")

        except ValueError as ve:
            print(f"ValueError: {ve}")
            raise CustomException(ve, sys)

        except FileNotFoundError as fnfe:
            print(f"FileNotFoundError: {fnfe}")
            raise CustomException(fnfe, sys)

        except BotoCoreError as bce:
            print(f"BotoCoreError while sending email: {bce}")
            raise CustomException(bce, sys)

        except ClientError as ce:
            print(f"ClientError while sending email: {ce}")
            raise CustomException(ce, sys)

        except Exception as e:
            print(f"Unexpected error while sending email: {e}")
            raise CustomException(e, sys)