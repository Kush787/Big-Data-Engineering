# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,DoubleType,DateType

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,regexp_replace,sha2,lit,when,to_date

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

# COMMAND ----------

account_schema = StructType(
    fields = [
        StructField("acc_id",StringType(),False),
        StructField("mem_id",StringType(),False),
        StructField("loan_id",StringType(),False),
        StructField("grade",StringType(),True),
        StructField("sub_grade",StringType(),True),
        StructField("emp_title",StringType(),True),
        StructField("emp_length",StringType(),True),
        StructField("home_ownership",StringType(),True),
        StructField("annual_inc",FloatType(),True),
        StructField("verification_status",StringType(),True),
        StructField("tot_hi_cred_lim",FloatType(),True),
        StructField("application_type",StringType(),True),
        StructField("annual_inc_joint",FloatType(),True),
        StructField("verification_status_joint",StringType(),True)
        
    ]
)

# COMMAND ----------

account_df = spark.read \
    .option("header",True) \
        .schema(account_schema) \
            .csv(f"{raw_file_path}account_details.csv")

# COMMAND ----------

display(account_df)

# COMMAND ----------

replace_df = account_df.withColumn("emp_length", when(col("emp_length")==lit("n/a"),lit("null")).otherwise(col("emp_length")))

display(replace_df)

# COMMAND ----------

replace_df_1yr = replace_df.withColumn("emp_length", when(col("emp_length")==lit("< 1 year"),lit("1")).otherwise(col("emp_length")))

display(replace_df_1yr)

# COMMAND ----------

replace_df_10yr = replace_df_1yr.withColumn("emp_length", when(col("emp_length")==lit("10+ years"),lit("10")).otherwise(col("emp_length")))

display(replace_df_10yr)

# COMMAND ----------

string_to_remove = "years"
replace_value_years = replace_df_10yr.withColumn("emp_length",regexp_replace(replace_df_10yr["emp_length"],string_to_remove,""))
display(replace_value_years)

# COMMAND ----------

string_to_remove = "year"
clean_df = replace_value_years.withColumn("emp_length",regexp_replace(replace_value_years["emp_length"],string_to_remove,""))
display(clean_df)

# COMMAND ----------

final_clean_df = clean_df.replace("null",None)

# COMMAND ----------

account_df_ingestDate = final_clean_df.withColumn("ingest_date",current_timestamp())

# COMMAND ----------

account_df_key = account_df_ingestDate.withColumn("account_key",sha2(concat(col("acc_id"),col("mem_id"),col("loan_id")),256))
display(account_df_key)

# COMMAND ----------

account_df_renamed = account_df_key.withColumnRenamed("acc_id","account_id") \
    .withColumnRenamed("mem_id","member_id") \
        .withColumnRenamed("emp_title","employee_designation") \
            .withColumnRenamed("emp_length","employee_experience") \
                .withColumnRenamed("annual_inc","annual_income") \
                    .withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit") \
                        .withColumnRenamed("annual_inc_joint","annual_income_joint")


# COMMAND ----------

display(account_df_renamed)

# COMMAND ----------

account_df_renamed.createOrReplaceTempView("temp_table")
final_df = spark.sql("select account_key,ingest_date,account_id,member_id,loan_id,grade,sub_grade,employee_designation,employee_experience,home_ownership,annual_income,verification_status,total_high_credit_limit,application_type,annual_income_joint,verification_status_joint from temp_table")
display(final_df)

# COMMAND ----------

final_df.write.options(header='True').mode('append').parquet(f"{cleaned_files_path}account_details")

# COMMAND ----------

dbutils.notebook.exit("executed account job")