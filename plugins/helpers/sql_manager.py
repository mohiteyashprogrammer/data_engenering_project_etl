import os
import sys
import pandas as pd 
import numpy as np 
import pymysql
import mysql.connector
from dotenv import load_dotenv
load_dotenv()
from helpers.aws_simple_email_service import ErrorEmailSender
from helpers.exception import CustomException

email_sender = ErrorEmailSender(
    region_name=os.getenv("AWS_REGION_NAME"),
    source_email=os.getenv("SOURCE_EMAIL"),
    destination_email=os.getenv("DESTINATION_EMAIL"))


class DatabaseConnection:

    def __init__(self, host, user, password, database):
        try:
            if not all([host, user, password, database]):
                raise ValueError("All database connection parameters (host, user, password, database) must be provided.")
            
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.cursor = self.connection.cursor()
            print("Connected to MySQL Database")

        except mysql.connector.Error as db_error:
            email_sender.send_error_email(
                message_to_send=f"MySQL Database connection error: {str(db_error)}")
            raise CustomException(db_error, sys)

        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Unexpected error occurred while connecting to MySQL_DB: {str(e)}")
            raise CustomException(e, sys)

    def execute_query(self, query):
        """
        Executes a SQL query and returns the results.

        Args:
            query (str): SQL query to execute.

        Returns:
            list: Query results.
        """
        if not query.strip():
            raise ValueError("Query string cannot be empty or whitespace.")

        try:
            self.cursor.execute(query)
            self.connection.commit()
            results = self.cursor.fetchall()
            return results

        except mysql.connector.Error as db_error:
            email_sender.send_error_email(
                message_to_send=f"MySQL error executing query: {str(db_error)}")
            raise CustomException(db_error, sys)

        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Unexpected error while executing query: {str(e)}")
            raise CustomException(e, sys)

    def execute_query_to_dataframe(self, query):
        """
        Executes a SQL query and returns the results as a pandas DataFrame.

        Args:
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame: DataFrame containing the query results.
        """
        if not query.strip():
            raise ValueError("Query string cannot be empty or whitespace.")

        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            if not results:
                return pd.DataFrame()  # Return an empty DataFrame if no results are fetched.

            columns = self.cursor.column_names
            df = pd.DataFrame(results, columns=columns)
            return df

        except mysql.connector.Error as db_error:
            email_sender.send_error_email(
                message_to_send=f"MySQL error executing query: {str(db_error)}")
            raise CustomException(db_error, sys)

        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Unexpected error while executing query: {str(e)}")
            raise CustomException(e, sys)

    def insert_data_batch(self, query, data, batch_size):
        """
        Insert data into the database in batches.

        Args:
            query (str): The SQL query string.
            data (list of tuple): Data to be inserted.
            batch_size (int): Batch size for insertion.

        Returns:
            None
        """
        if not query.strip():
            raise ValueError("Query string cannot be empty or whitespace.")
        
        if not isinstance(data, list) or not all(isinstance(row, tuple) for row in data):
            raise ValueError("Data must be a list of tuples.")

        if batch_size <= 0:
            raise ValueError("Batch size must be a positive integer.")

        error_rows = []

        try:
            for i in range(0, len(data), batch_size):
                batch_data = data[i:i + batch_size]
                try:
                    self.cursor.executemany(query, batch_data)
                    self.connection.commit()
                except mysql.connector.Error as db_error:
                    error_rows.extend(batch_data)
                    if len(error_rows) > 3:
                        error_df = pd.DataFrame(error_rows, columns=[f"Column_{j}" for j in range(len(data[0]))])
                        print("Error rows DataFrame:")
                        print(error_df)
                        email_sender.send_error_email(
                            message_to_send=f"Error inserting batch data into the table: {str(db_error)}",
                            attachment=error_df
                        )
                        break  # Exit loop if error rows exceed 3
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Unexpected error during batch data insertion: {str(e)}")
            raise CustomException(e, sys)

    def save_to_csv(self, data, file_name, path):
        """
        Saves data to a CSV file.

        Args:
            data (pd.DataFrame): Data to be saved.
            file_name (str): Name of the CSV file.
            path (str): Directory path to save the file.

        Returns:
            None
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be a pandas DataFrame.")

        if not file_name.strip() or not file_name.endswith('.csv'):
            raise ValueError("File name must be a non-empty string and end with '.csv'.")

        if not path.strip():
            raise ValueError("Path cannot be empty or whitespace.")

        try:
            if not os.path.exists(path):
                os.makedirs(path)

            full_file_path = os.path.join(path, file_name)
            data.to_csv(full_file_path, index=False)
            print(f"File '{file_name}' saved to disk in the '{path}' folder.")

        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error saving data to '{path}' folder: {str(e)}")
            raise CustomException(e, sys)

    def close(self):
        """
        Closes the database connection.

        Returns:
            None
        """
        try:
            if self.connection.is_connected():
                self.cursor.close()
                self.connection.close()
                print("MySQL connection closed.")
            else:
                print("MySQL connection is already closed.")
        except Exception as e:
            email_sender.send_error_email(
                message_to_send=f"Error closing MySQL connection: {str(e)}")
            raise CustomException(e, sys)