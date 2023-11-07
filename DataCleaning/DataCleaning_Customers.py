# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

from pyspark.sql.functions import concat, current_timestamp,sha2,col

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

# COMMAND ----------

customer_schema = StructType(
    fields= [
        StructField("cust_id",StringType(), True),
        StructField("mem_id",StringType(), True),
        StructField("fst_name",StringType(), True),
        StructField("lst_name",StringType(), True),
        StructField("prm_status",StringType(), True),
        StructField("age",IntegerType(), True),
        StructField("state",StringType(), True),
        StructField("country",StringType(), True)
    ]
)

# COMMAND ----------

customer_df = spark.read \
    .option("header",True) \
        .schema(customer_schema) \
            .csv(f"{raw_file_path}loan_customer_data.csv")

# COMMAND ----------

display(customer_df)

# COMMAND ----------

customer_df_changed = customer_df.withColumnRenamed("cust_id","customer_id") \
    .withColumnRenamed("mem_id","member_id") \
        .withColumnRenamed("fst_name","first_name") \
            .withColumnRenamed("lst_name","last_name") \
                .withColumnRenamed("prm_status","permanent_status")

# COMMAND ----------

display(customer_df_changed)

# COMMAND ----------

customer_df_ingestdate = customer_df_changed.withColumn("ingest_date",current_timestamp())

# COMMAND ----------

display(customer_df_ingestdate)

# COMMAND ----------

customer_df_final = customer_df_ingestdate.withColumn("customer_key", sha2(concat(col("member_id"),col("age"),col("state")), 256))

# COMMAND ----------

display(customer_df_final)


# COMMAND ----------

customer_df_final.createOrReplaceTempView("temp_table")
display_df = spark.sql("select customer_key,ingest_date,customer_id,member_id,first_name,last_name,permanent_status,age,state,country from temp_table")
display(display_df)

# COMMAND ----------

display_df.write.options(header='True').mode("append").parquet(f"{cleaned_files_path}customer_details")

# COMMAND ----------

dbutils.notebook.exit("executed customers job")