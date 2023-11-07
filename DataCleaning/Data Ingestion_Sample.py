# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

customer_schema = StructType(fields=[
    StructField("cust_id", StringType(),True),
    StructField("mem_id", StringType(),True),
    StructField("fst_name", StringType(),True),
    StructField("lst_name", StringType(),True),
    StructField("prm_status", StringType(),True),
    StructField("age", StringType(),True),
    StructField("state", StringType(),True),
    StructField("country", StringType(),True),

])

# COMMAND ----------

import datetime
def ingestData():
    load_date = datetime.datetime.today()-datetime.timedelta(days=1)
    load_date_formatted = load_date.strftime('%d%m%Y')
    return load_date_formatted

# COMMAND ----------

date_loaded_on = ingestData()
file_load_path = "loan_customer_data_"+str(date_loaded_on)+".csv"
final_file_path = "/mnt/kushagrafinance/raw-data/"+file_load_path
print(final_file_path)



# COMMAND ----------

customers_df = spark.read \
    .option("header",True) \
        .schema(customer_schema) \
            .csv(final_file_path)



# COMMAND ----------

display(customers_df)

# COMMAND ----------

