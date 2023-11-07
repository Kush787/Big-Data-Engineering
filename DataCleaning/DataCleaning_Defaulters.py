# Databricks notebook source
from pyspark.sql.types import StructField,StructType,StringType,FloatType,IntegerType,DoubleType

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,current_timestamp,col,concat,lit,when,sha2

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

# COMMAND ----------

loan_default_schema = StructType(
    fields=[
        StructField("loan_id",StringType(),False),
        StructField("mem_id",StringType(),False),
        StructField("def_id",StringType(),False),
        StructField("delinq_2yrs",IntegerType(),True),
        StructField("delinq_amnt",FloatType(),True),
        StructField("pub_rec",IntegerType(),True),
        StructField("pub_rec_bankruptcies",IntegerType(),True),
        StructField("inq_last_6mths",IntegerType(),True),
        StructField("total_rec_late_fee",FloatType(),True),
        StructField("hardship_flag",StringType(),True),
        StructField("hardship_type",StringType(),True),
        StructField("hardship_length",IntegerType(),True),
        StructField("hardship_amount",FloatType(),True)
        
    ]
)

# COMMAND ----------

loan_default_df = spark.read \
    .option("header",True) \
        .schema(loan_default_schema) \
            .csv(f"{raw_file_path}loan_defaulters.csv")

# COMMAND ----------

loan_defaulter_ingestDate = loan_default_df.withColumn("ingest_date",current_timestamp())
display(loan_defaulter_ingestDate)

# COMMAND ----------

loan_default_df_key = loan_defaulter_ingestDate.withColumn("loan_default_key",sha2(concat(col("loan_id"),col("mem_id"),col("def_id")), 256))

display(loan_default_df_key)

# COMMAND ----------

df_null = loan_default_df_key.replace("null",None)
display(df_null)

# COMMAND ----------

df_null_renamed = df_null.withColumnRenamed("mem_id","member_id") \
    .withColumnRenamed("def_id","loan_defaulter_id") \
        .withColumnRenamed("delinq_2yrs","defaulters_2yrs") \
            .withColumnRenamed("delinq_amnt","defaulters_amount") \
                .withColumnRenamed("pub_rec","public_records") \
                    .withColumnRenamed("pub_rec_bankruptcies","public_record_bankruptcies") \
                        .withColumnRenamed("inq_last_6mths","enquiries_6mnths") \
                            .withColumnRenamed("total_rec_late_fee","late_fee") 

display(df_null_renamed)

# COMMAND ----------

df_null_renamed.createOrReplaceTempView("temp_table")
display_df = spark.sql("select loan_default_key,ingest_date,loan_id,member_id,loan_defaulter_id,defaulters_2yrs,defaulters_amount,public_records,public_record_bankruptcies,enquiries_6mnths,late_fee,hardship_flag,hardship_type,hardship_length,hardship_amount from temp_table")

display(display_df)

# COMMAND ----------

display_df.write.options(header='True').mode("append").parquet(f"{cleaned_files_path}loan-defaulters")

# COMMAND ----------

dbutils.notebook.exit("executed defaulters job")

# COMMAND ----------

