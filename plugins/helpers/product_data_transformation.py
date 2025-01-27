import csv
import random
import string
import time
from datetime import datetime
import pandas as pd

def process_data(dataframe):
    """
    Processes and transforms the input DataFrame with additional conditions and error handling.
    """
    try:
        # Validate input DataFrame
        if not isinstance(dataframe, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")
        if dataframe.empty:
            raise ValueError("Input DataFrame is empty.")
        
        # Check for required columns
        required_columns = [
            'exported', 'productid', 'product', 'price', 'depacher', 'arrival',
            'dateofcreation', 'location', 'age', 'group', 'usage', 'product_age'
        ]
        if not all(col in dataframe.columns for col in required_columns):
            raise ValueError(f"Input DataFrame must contain the following columns: {required_columns}")

        def transform_data(df_to_transform):
            """
            Applies transformations to the DataFrame.
            """
            try:
                current_time = datetime.now()
                df_to_transform['transformation_date'] = current_time.strftime('%Y-%m-%d')
                df_to_transform['transformation_time'] = current_time.strftime('%H:%M:%S')
                
                # Add transformations
                df_to_transform['price_category'] = df_to_transform['price'].apply(
                    lambda x: 'Low' if x < 200 else 'Medium' if x < 600 else 'High'
                )
                df_to_transform['transit_time'] = df_to_transform.apply(
                    lambda row: abs(int(row['depacher'].split('-')[-1]) - int(row['arrival'].split('-')[-1])), axis=1
                )
                df_to_transform['normalized_product_age'] = (
                    (df_to_transform['product_age'] - df_to_transform['product_age'].min()) /
                    (df_to_transform['product_age'].max() - df_to_transform['product_age'].min())
                )
                df_to_transform['region'] = df_to_transform['location'].apply(
                    lambda loc: 'North' if loc in ['Delhi', 'Mumbai'] else 'South'
                )
                df_to_transform['export_flag'] = 1
                df_to_transform['high_value_flag'] = df_to_transform.apply(
                    lambda row: 1 if row['price'] > 600 and row['usage'] == 'High' else 0, axis=1
                )
                df_to_transform['age_group'] = df_to_transform['age'].apply(
                    lambda age: 'Youth' if age < 30 else 'Adult' if age < 50 else 'Senior'
                )
                return df_to_transform
            except Exception as e:
                print(f"Error during data transformation: {e}")
                raise

        # Split the data into two DataFrames
        try:
            exported_yes_df = dataframe[dataframe['exported'] == 'Yes'].copy()
            exported_no_df = dataframe[dataframe['exported'] == 'No'].copy()
        except KeyError as e:
            print(f"Error splitting DataFrame: {e}")
            raise

        # Transform the `exported == Yes` DataFrame
        try:
            transformed_exported_yes_df = transform_data(exported_yes_df)
        except Exception as e:
            print(f"Error transforming 'exported == Yes' DataFrame: {e}")
            raise

        # Simulate processing `exported == No` after 5 minutes
        print("Waiting 5 minutes to process 'exported == No' rows...")
        time.sleep(60)  # Simulates 5 minutes delay

        # Update `exported == No` DataFrame and process
        try:
            current_time = datetime.now()
            exported_no_df['exported'] = 'Yes'
            exported_no_df['dateofcreation'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error updating 'exported == No' DataFrame: {e}")
            raise

        # Apply transformations to `exported == No`
        try:
            transformed_exported_no_df = transform_data(exported_no_df)
        except Exception as e:
            print(f"Error transforming 'exported == No' DataFrame: {e}")
            raise

        # Concatenate the transformed DataFrames
        try:
            final_df = pd.concat([transformed_exported_yes_df, transformed_exported_no_df])
            final_df = final_df.sample(frac=1).reset_index(drop=True)  # Shuffle the DataFrame
        except Exception as e:
            print(f"Error concatenating DataFrames: {e}")
            raise

        return final_df

    except Exception as e:
        print(f"An error occurred during processing: {e}")
        raise