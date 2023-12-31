# Databricks notebook source
storage_account_name = "kushagrafinance"
client_id            = ""
tenant_id            = ""
client_secret        = ""


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

#test blob created to mount
mount_adls("raw-data")
mount_adls("cleaned-data")
mount_adls("processed-data")

# COMMAND ----------

mount_adls("cleaned-data")
mount_adls("processed-data")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/kushagrafinance")

# COMMAND ----------

#listing files inside raw-data container
dbutils.fs.ls("/mnt/datasetbigdata/raw-data/lending_loan")

# COMMAND ----------

