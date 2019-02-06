# Databricks notebook source
# MAGIC %md
# MAGIC UNMOUNT Databricks File Systems

# COMMAND ----------

import os
MOUNTP = os.environ['MOUNT_DATA_PATH']
dbutils.fs.unmount(MOUNTP)

# COMMAND ----------


