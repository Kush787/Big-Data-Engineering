# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,FloatType,DateType

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp,regexp_replace,lit,to_date,sha2

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

# COMMAND ----------

loan_schema = StructType(
    fields = [
        StructField("loan_id",StringType(),False),
        StructField("mem_id",StringType(),False),
        StructField("acc_id",StringType(),False),
        StructField("loan_amnt",DoubleType(),True),
        StructField("fnd_amt",DoubleType(),True),
        StructField("term",StringType(),True),
        StructField("interest",StringType(),True),
        StructField("installment",FloatType(),True),
        StructField("issue_date",StringType(),True),
        StructField("loan_status",StringType(),True),
        StructField("purpose",StringType(),True),
        StructField("title",StringType(),True),
        StructField("disbursement_method",StringType(),True),
        
        
    ]
)

# COMMAND ----------

loan_df = spark.read \
    .option("header",True) \
        .schema(loan_schema) \
            .csv(f"{raw_file_path}loan_details.csv")

# COMMAND ----------

display(loan_df)

# COMMAND ----------

loan_df.createOrReplaceTempView("loan_table")
loan_sql = spark.sql("select * from loan_table")
display(loan_sql)

# COMMAND ----------

string_to_remove = "months"
clean_term_df = loan_df.withColumn("term",regexp_replace(loan_df["term"],string_to_remove,""))
display(clean_term_df)


# COMMAND ----------

string1_to_remove = "%"
clean_interest_df = clean_term_df.withColumn("interest",regexp_replace(clean_term_df["interest"],string1_to_remove,""))
display(clean_interest_df)

# COMMAND ----------

loan_rename_df = clean_interest_df.withColumnRenamed("mem_id","member_id") \
    .withColumnRenamed("acc_id","account_id") \
        .withColumnRenamed("loan_amnt","loan_amount") \
            .withColumnRenamed("fnd_amt","funded_amount")

# COMMAND ----------

loan_df_ingestDate = loan_rename_df.withColumn("ingest_date",current_timestamp())
display(loan_df_ingestDate)

# COMMAND ----------

loan_df_key = loan_df_ingestDate.withColumn("loan_key",sha2(concat(col("loan_id"),col("member_id"),col("loan_amount")),256))

# COMMAND ----------

loan_df_key.createOrReplaceTempView("df_null")
df_null = spark.sql("select * from df_null where interest='null'")
display(df_null)

# COMMAND ----------

loan_df_key.createOrReplaceTempView("df_null")
df_null = spark.sql("select * from df_null where interest is null")
display(df_null)

# COMMAND ----------

final_df = loan_df_key.replace("null",None)

# COMMAND ----------

final_df.createOrReplaceTempView("loan")
loan = spark.sql("select * from loan where interest is null")
display(loan)

# COMMAND ----------

final_df.createOrReplaceTempView("loan")
loan = spark.sql("select * from loan where term=36 and interest > 5.0")
display(loan)


# COMMAND ----------

final_df.createOrReplaceTempView("temp_table")
display_df = spark.sql("select loan_key,ingest_date,loan_id,member_id,account_id,loan_amount,funded_amount,term,interest,installment,issue_date,loan_status,purpose,title,disbursement_method from temp_table")
display(display_df)


# COMMAND ----------

display_df.write.options(header='True').mode('append').parquet(f"{cleaned_files_path}loan_details")

# COMMAND ----------

dbutils.notebook.exit("executed loan job")