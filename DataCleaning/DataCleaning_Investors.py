# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,DoubleType

# COMMAND ----------

from pyspark.sql.functions import sha2,regexp_replace,col,current_timestamp,concat

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

# COMMAND ----------

investors_schema = StructType(
    fields = [
        StructField("investor_loan_id",StringType(),False),
        StructField("loan_id",StringType(),False),
        StructField("investor_id",StringType(),False),
        StructField("loan_fund_amnt",DoubleType(),True),
        StructField("investor_type",StringType(),False),
        StructField("age",IntegerType(),False),
        StructField("state",StringType(),False),
        StructField("country",StringType(),False)
    ]
)

# COMMAND ----------

investors_df = spark.read \
    .option("header",True) \
        .schema(investors_schema) \
            .csv(f"{raw_file_path}loan_investors.csv")

display(investors_df)

# COMMAND ----------

investors_df_ingestDate = investors_df.withColumn("ingest_date",current_timestamp())
display(investors_df_ingestDate)

# COMMAND ----------

investors_key = investors_df_ingestDate.withColumn("investors_key",sha2(concat(col("investor_loan_id"),col("loan_id"),col("investor_type")),256))

display(investors_key)

# COMMAND ----------

df_null = investors_key.replace("null",None)
display(df_null)

# COMMAND ----------

df_null.createOrReplaceTempView("temp_table")
display_df = spark.sql("select investors_key,ingest_date,investor_loan_id,loan_id,investor_id,loan_fund_amnt,investor_type,age,state,country from temp_table")
display(display_df)

# COMMAND ----------

display_df.write.options(header='True').mode("append").parquet(f"{cleaned_files_path}loan_investors")

# COMMAND ----------

dbutils.notebook.exit("executed investors job")