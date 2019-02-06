# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebeook mount Azure Data Lake Gen2
# MAGIC 
# MAGIC ### Please create secrets before you run this notebook.
# MAGIC databricks secrets create-scope --scope msscope --initial-manage-principal users
# MAGIC 
# MAGIC databricks secrets list-scopes
# MAGIC 
# MAGIC databricks secrets put --scope msscope --key storage_account --string-value {Azure Storage Account}
# MAGIC 
# MAGIC databricks secrets put --scope msscope --key storage_container --string-value {Storage Container}
# MAGIC 
# MAGIC databricks secrets put --scope msscope --key client_id --string-value {Application ID}
# MAGIC 
# MAGIC databricks secrets put --scope msscope --key client_secret --string-value {Key}
# MAGIC 
# MAGIC databricks secrets put --scope msscope --key directory_id --string-value {Key}  
# MAGIC ***https://login.microsoftonline.com/{AADDirectoryID}/oauth2/token
# MAGIC 
# MAGIC databricks secrets list --scope mtiscope

# COMMAND ----------

import os

# COMMAND ----------

MOUNTP = os.environ['MOUNT_DATA_PATH']
STORAGE_ACCOUNT = dbutils.secrets.get(scope="msscope", key="storage_account")
CONTAINER = dbutils.secrets.get(scope="msscope", key="storage_container")
DIRECTORY_ID = dbutils.secrets.get(scope="msscope", key="directory_id")
ENDPOINT = "https://login.microsoftonline.com/{directry_id}/oauth2/token".format(directry_id=DIRECTORY_ID)

# COMMAND ----------

print(MOUNTP)
print(STORAGE_ACCOUNT)
print(CONTAINER)
print(DIRECTORY_ID)
print(ENDPOINT)

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "msscope", key = "client_id"),
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "msscope", key = "client_secret"),
    "fs.azure.account.oauth2.client.endpoint": ENDPOINT,
    "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# Generate ADLS Gen2 connection string
source_str = "abfss://{container}@{storage_account}.dfs.core.windows.net/".format(container=CONTAINER, storage_account=STORAGE_ACCOUNT)

dbutils.fs.mount(
    source = source_str,
    mount_point = MOUNTP,
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("file:/dbfs" + MOUNTP)

# COMMAND ----------


