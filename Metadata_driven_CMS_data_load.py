# Databricks notebook source
# DBTITLE 1,Imports
import os
import requests
import pandas as pd
import re
import smtplib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Metadata table for csv files
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.job_log (
# MAGIC     file_name STRING NOT NULL,
# MAGIC     url STRING NOT NULL,
# MAGIC     download_timestamp timestamp NOT NULL,
# MAGIC     Modified_date date NOT NULL
# MAGIC )

# COMMAND ----------

# Configuration
API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DATA_DIR = "data/"

# COMMAND ----------

# DBTITLE 1,Common Functions
# Load metadata to track run history and modified timestamps
def read_metadata():
    metadata_df = spark.sql("select * from default.job_log")
    return metadata_df

# Fetch datasets related to 'Hospitals'
def fetch_datasets():
    response = requests.get(API_URL)
    response.raise_for_status()  # Ensure we got a successful response
    data = response.json()
    filtered_data = [item for item in data if item.get('theme')[0] == 'Hospitals']
    return filtered_data

# Convert column names to snake_case
def to_snake_case(column_name):
    # Replace special characters with space
    column_name = re.sub(r"[^\w\s]", " ", column_name)
    # Replace spaces or multiple underscores with a single underscore
    column_name = re.sub(r"\s+", "_", column_name)
    # Convert to lowercase
    column_name = column_name.lower()
    return column_name.strip("_")
   
# # write metadata to track run history and modified timestamps
def insert_metadata(csv_filename, csv_url, last_modified, table_name="job_log"):
    """
    Insert metadata such as csv_filename, csv_url, download_timestamp, and last_modified into a specified table.
    Args:
    - csv_filename: Name of the CSV file.
    - csv_url: URL to the CSV file.
    - last_modified: Last modified date of the file.
    - table_name: The name of the table where data will be inserted (default is "your_table_name").
    """
    # Get current timestamp for download timestamp
    download_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Construct the dynamic SQL query
    insert_query = f"""
    INSERT INTO {table_name} (file_name, url, download_timestamp, Modified_date)
    VALUES ('{csv_filename}', '{csv_url}', '{download_timestamp}', '{last_modified}')
    """
    
    # Execute the dynamic SQL query using spark.sql()
    spark.sql(insert_query)

    return (f"Insert metadata executed successfully: {insert_query}")

# COMMAND ----------

def process_csv(dataset):
    # print(dataset)
    csv_url = dataset['distribution'][0]['downloadURL']
    csv_filename = os.path.basename(csv_url)
      # Check if file has been modified since last run
    last_modified = dataset.get('modified', '')
    #print(last_modified)

    metadata_df = read_metadata()

    # Filter DataFrame for the given csv_filename
    filtered_df = metadata_df.filter(metadata_df.file_name == csv_filename)

    # Check if the csv_filename exists and compare modified_date to check if file is modified.
    if filtered_df.count() > 0:
        # Extract the modified date from the filtered DataFrame
        modified_date1 = filtered_df.collect()[0]['Modified_date']
        modified_date = modified_date1.strftime('%Y-%m-%d')
        if modified_date != last_modified:
            print(f"The last modified date for {csv_filename} is different. Expected: {last_modified}, Found: {modified_date}")
            #Download and process CSV
            print(f"Downloading and processing {csv_url}...")
            response = requests.get(csv_url)
            response.raise_for_status()

            # Save the CSV
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(DATA_DIR+csv_filename, 'wb') as file:
                file.write(response.content)

            # Process the CSV into DataFrame
            df = pd.read_csv(DATA_DIR+csv_filename, dtype={"column_name": str},low_memory=False)

            # Convert column names to snake_case
            df.columns = [to_snake_case(col) for col in df.columns]
            #print(df)
            spark_df = spark.createDataFrame(df)
            #display(spark_df)
            insert_metadata(csv_filename, csv_url, last_modified)
            
        else:
            print(f"Dataset {csv_filename} not modified, skipping download process....")
    else:
        print(f"CSV file '{csv_filename}' not found in the DataFrame.")
        #Download and process CSV
        print(f"Downloading and processing {csv_url}...")
        response = requests.get(csv_url)
        response.raise_for_status()

        # Save the CSV
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(DATA_DIR+csv_filename, 'wb') as file:
            file.write(response.content)

        # Process the CSV into DataFrame
        df = pd.read_csv(DATA_DIR+csv_filename, dtype={"column_name": str},low_memory=False)

        # Convert column names to snake_case
        df.columns = [to_snake_case(col) for col in df.columns]
        #print(df)
        spark_df = spark.createDataFrame(df)
        #display(spark_df)
        insert_metadata(csv_filename, csv_url, last_modified)
    return csv_filename  
  


# COMMAND ----------

# Main function to download and process datasets
def main():
    # Fetch datasets
    datasets = fetch_datasets()
    
    # Process datasets in parallel
    with ThreadPoolExecutor() as executor:
        files = list(executor.map(process_csv, datasets))


if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from job_log
# MAGIC --truncate table job_log

# COMMAND ----------

# DBTITLE 1,CSV files downloaded
# MAGIC %sh
# MAGIC
# MAGIC pwd
# MAGIC cd /databricks/driver/data
# MAGIC ls
