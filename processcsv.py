# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebeook process CSV Data in Azure Data Lake Gen2 and store data in Cosmos DB. 
# MAGIC There are 3 input parameters that you need to pass. 
# MAGIC 
# MAGIC ### Please create secrets before you run this notebook.
# MAGIC databricks secrets create-scope --scope msscope --initial-manage-principal users
# MAGIC 
# MAGIC databricks secrets put --scope msscope --key cosmosdb_key --string-value {Azure Cosmos DB Key}
# MAGIC 
# MAGIC databricks secrets put --scope msscope --key cosmosdb_account --string-value {Azure Cosmos DB Account}

# COMMAND ----------

import numpy as np
import pandas as pd
import re
import os
import glob
import sys
from pyspark.sql import DataFrame, SQLContext
from pytz import timezone
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Input parameters to select input folder path and cosmos db.
dbutils.widgets.text("inputfolderpath","/input/")
dbutils.widgets.text("cosmosdb_database", "msdbid")
dbutils.widgets.text("cosmosdb_collection", "mscolid")

# COMMAND ----------

# input parameters
inputfolderpath = dbutils.widgets.get("inputfolderpath")
cosmosdb_database = dbutils.widgets.get("cosmosdb_database")
cosmosdb_collection = dbutils.widgets.get("cosmosdb_collection")

# COMMAND ----------

MOUNTP = os.environ['MOUNT_DATA_PATH']
dbutils.fs.ls("file:/dbfs/" + MOUNTP + inputfolderpath)

# COMMAND ----------

def read_process_csv(input_path: str):
    """Read csv as pandas df

    Read csv and process data by DB format.
    Remove space in csv and add datetime columns.

    Args:
        input_path (str): path string ofcsv files.

    Returns:
        obj: pandas.DataFrame
    """
    utc_now = timezone('UTC').localize(datetime.utcnow())
    now_str = utc_now.strftime('%Y-%m-%d %H:%M:%S.%f %z')
    df = pd.read_csv(input_path, engine="python", header=None, encoding="shift-jis")
    #df = df.replace(r'\s+', np.nan, regex=True)
    df = df.astype(object).where(pd.notnull(df), None)
    df = df.reset_index()
    df["CreatedAt"] = now_str
    return df

# COMMAND ----------

def read_process_dir(input_path: str):
    """Read dir

    Read dir.
    Remove space in csv and add datetime columns.

    Args:
        input_path (str): path string of csv files.

    Returns:
        obj: pandas.DataFrame
    """
    files = os.listdir(input_path)
    df_csv = pd.DataFrame()

    for filename in files:
        filepath = os.path.join(input_path, filename)
        
        if os.path.isfile(filepath):
            df_row = read_process_csv(filepath)
            df_csv = df_csv.append(df_row, ignore_index=True)
    return df_csv

# COMMAND ----------

'''
def read_process_dir2(input_path: str):
    allFiles = glob.glob(input_path + "/*.ctl")
    frame = pd.DataFrame()
    list_ = []
    for file_ in allFiles:
        print(file_)
        #df = pd.read_csv(file_,index_col=None, header=0)
        df = read_process_csv(file_)
        list_.append(file_)
    return pd.concat(list_)
'''

# COMMAND ----------

def converToSparkDF(pdf):
  # Spark DataFrame to Pands  spark.conf.set("spark.sql.execution.arrow.enabled", "true")
  #schema = StructType([StructField("ANNOUNCE_DATETIME", (), True), StructField("score", DoubleType(), True)])
  
  # Define the schema to apply to the data...
  schema = StructType([
    StructField("Index", StringType()),
    StructField("UPDATE_DATETIME", StringType()),
    StructField("REGION", StringType()),
    StructField("COUNTRY", StringType())
  ])
  
  df = spark.createDataFrame(pdf, schema)
  return df

# COMMAND ----------

def save_cosmosdb(sdf: DataFrame, **writeconfig):
    sdf.write.format("com.microsoft.azure.cosmosdb.spark")\
        .mode('overwrite')\
        .options(writeconfig).save()

# COMMAND ----------

pandas_df = read_process_dir("/dbfs" + MOUNTP + inputfolderpath)

# COMMAND ----------

print(pandas_df)

# COMMAND ----------

sparkdf = converToSparkDF(pandas_df)

# COMMAND ----------

display(sparkdf)

# COMMAND ----------

COSMOSDB_ACCOUNT = dbutils.secrets.get(scope = "msscope", key = "cosmosdb_account") 
ENDPOINT = "https://{cosmosdb_account}.documents.azure.com:443/".format(cosmosdb_account=COSMOSDB_ACCOUNT)

writeConfig = {
 "Endpoint" : ENDPOINT,
 "Masterkey" : dbutils.secrets.get(scope = "msscope", key = "cosmosdb_key"),
 "Database" : cosmosdb_database,
 "Collection" : cosmosdb_collection,
 "Upsert" : "true"
}

sparkdf.write.format("com.microsoft.azure.cosmosdb.spark").mode("append").options(**writeConfig).save()

# COMMAND ----------


