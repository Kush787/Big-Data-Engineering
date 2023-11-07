# Databricks notebook source
raw_file_path = "/mnt/kushagrafinance/raw-data/Lending_Loan/"

# COMMAND ----------

processed_file_path = "/mnt/kushagrafinance/processed-data/Lending_Loan/"

# COMMAND ----------

cleaned_files_path = "/mnt/kushagrafinance/cleaned-data/Lending_Loan/"

# COMMAND ----------

dbfs_file_path = "/FileStore/tables/"

# COMMAND ----------

clean_script_path = "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/DataCleaning"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def ingestDate(input_df):
    date_df = input.df.withColumn("ingest_date",current_timestamp())
    return date_df