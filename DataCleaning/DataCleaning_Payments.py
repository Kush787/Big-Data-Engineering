# Databricks notebook source
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType,DoubleType,DateType

# COMMAND ----------

from pyspark.sql.functions import concat,current_timestamp,sha2,col

# COMMAND ----------

# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"
# MAGIC

# COMMAND ----------

payments_schema = StructType (
    fields = [
        StructField("loan_id",StringType(),False),
        StructField("mem_id",StringType(),False),
        StructField("latest_transaction",StringType(),False),
        StructField("funded_amnt_inv",DoubleType(),True),
        StructField("total_pymnt_rec",FloatType(),True),
        StructField("installment",FloatType(),True),
        StructField("last_pymnt_amnt",FloatType(),True),
        StructField("last_pymnt_d",DateType(),True),
        StructField("next_pymnt_d",DateType(),True),
        StructField("pymnt_method",StringType(),True),
    ]
)

# COMMAND ----------

payments_df = spark.read \
    .option("header",True) \
        .schema(payments_schema) \
            .csv(f"{raw_file_path}loan_payment.csv")

display(payments_df)

# COMMAND ----------

payments_df_ingestDate = payments_df.withColumn("ingest_date",current_timestamp())

# COMMAND ----------

payments_df_key = payments_df_ingestDate.withColumn("payments_key",sha2(concat(col("loan_id"),col("mem_id"),col("latest_transaction")),256))

display(payments_df_key)

# COMMAND ----------

null_df= payments_df_key.replace("null",None)

# COMMAND ----------

payments_df_renamed = null_df.withColumnRenamed("mem_id","member_id") \
    .withColumnRenamed("funded_amnt_inv","funded_amount_investor") \
        .withColumnRenamed("total_pymnt_rec","total_payment_recorded") \
            .withColumnRenamed("last_pymnt_amnt","last_payment_amount") \
                .withColumnRenamed("last_pymnt_d","last_payment_date") \
                    .withColumnRenamed("next_pymnt_d","next_payment_date") \
                        .withColumnRenamed("pymnt_method","payment_method")

display(payments_df_renamed)


# COMMAND ----------

payments_df_renamed.createOrReplaceTempView("temp_table")
display_df = spark.sql("select payments_key,ingest_date,loan_id,member_id,latest_transaction,funded_amount_investor,total_payment_recorded,installment,last_payment_amount,last_payment_date,next_payment_date,payment_method from temp_table")
display(display_df)


# COMMAND ----------

display_df.write.options(header='True').mode("append").parquet(f"{cleaned_files_path}loan_payment")

# COMMAND ----------

dbutils.notebook.exit("executed payments job")