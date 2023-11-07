# Databricks notebook source
# MAGIC %run "/Repos/Big_Data_Engineering_Project/Big-Data-Engineering/Lending_Loan/Adhoc_Files/Function_Variables"

# COMMAND ----------

status_customers = dbutils.notebook.run(f"{clean_script_path}DataCleaning_Customers",0)

# COMMAND ----------

if (status_customers == "executed customers job") : 
    print("customers job executed successfully")

# COMMAND ----------

status_account = dbutils.notebook.run(f"{clean_script_path}DataCleaning_Account_Details",0)

# COMMAND ----------

if (status_account == "executed account job"):
    print("accounts job executed successfully")

# COMMAND ----------

status_loan = dbutils.notebook.run(f"{clean_script_path}DataCleaning_Loan",0)

# COMMAND ----------

if (status_loan == "executed loan job"):
    print("loan job executed successfully")

# COMMAND ----------

status_payments = dbutils.notebook.run(f"{clean_script_path}DataCleaning_Payments",0)

# COMMAND ----------

if (status_payments == "executed payments job"):
    print("payments job executed successfully")

# COMMAND ----------

status_defaulters = dbutils.notebook.run(f"{clean_script_path}DataCleaning_Defaulters",0)

# COMMAND ----------

if (status_defaulters == "executed defaulters job"):
    print("defaulters job exeuted successfully")

# COMMAND ----------

status_investors = dbutils.notebook.run(f"{clean_script_path}DataCleaning_Investors",0)

# COMMAND ----------

if (status_investors == "executed investors job"):
    print("investors job executed successfully")

# COMMAND ----------

