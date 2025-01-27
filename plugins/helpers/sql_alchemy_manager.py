import os
import sys
import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
from helpers.aws_simple_email_service import ErrorEmailSender
from helpers.exception import CustomException

# Load environment variables
load_dotenv()

email_sender = ErrorEmailSender(
    region_name=os.getenv("AWS_REGION_NAME"),
    source_email=os.getenv("SOURCE_EMAIL"),
    destination_email=os.getenv("DESTINATION_EMAIL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_ID"))
class DatabaseManager:

    def __init__(self, connection_string):
        try:
            if not connection_string:
                raise ValueError("Database connection string is empty or invalid.")
            
            pymysql.install_as_MySQLdb()  # Install pymysql as MySQLdb for compatibility
            self.db_engine = create_engine(connection_string)
            self.conn = self.db_engine.connect()
            print("Database connection established.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while establishing database connection: {str(e)}"
            )
            raise CustomException(e, sys)
    
    def import_data_to_table(self, data, table_name):
        """
        Imports a DataFrame to a database table.

        Args:
            data (pd.DataFrame): The data to be imported.
            table_name (str): Name of the table in the database.
        """
        try:
            if data is None or data.empty:
                raise ValueError("Data is empty or not provided.")
            if not isinstance(data, pd.DataFrame):
                raise TypeError("Provided data is not a pandas DataFrame.")
            if not table_name:
                raise ValueError("Table name is empty or not provided.")
            
            data.to_sql(table_name, con=self.db_engine, if_exists='append', index=False)
            print(f"Data imported successfully to table '{table_name}'.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while importing data to table '{table_name}': {str(e)}"
            )
            raise CustomException(e, sys)

    def import_csv_to_table(self, file_path, table_name):
        """
        Imports data from a CSV file to a database table.

        Args:
            file_path (str): Path to the CSV file.
            table_name (str): Name of the table in the database.
        """
        try:
            if not file_path or not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file '{file_path}' does not exist.")
            if not table_name:
                raise ValueError("Table name is empty or not provided.")
            
            # Load CSV into a DataFrame
            df = pd.read_csv(file_path, encoding='latin1', sep=",")
            if df.empty:
                raise ValueError("CSV file is empty.")
            
            # Validate schema
            if not isinstance(df, pd.DataFrame):
                raise TypeError("Loaded file is not a valid DataFrame.")
            
            # Import data to database
            df.to_sql(table_name, con=self.db_engine, if_exists='replace', index=False)
            print(f"CSV data successfully imported to table '{table_name}'.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while importing CSV data to table '{table_name}': {str(e)}"
            )
            raise CustomException(e, sys)

    def close_connection(self):
        """
        Closes the database connection.
        """
        try:
            if self.conn.closed:
                print("Connection is already closed.")
            else:
                self.conn.close()
                print("Closed connection to database.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error occurred while closing the database connection: {str(e)}"
            )
            raise CustomException(e, sys)