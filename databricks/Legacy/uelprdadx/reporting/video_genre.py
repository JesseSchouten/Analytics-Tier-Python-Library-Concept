# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.functions import udf, array, lit, col

import sys
import os

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import hashlib
#dbutils.fs.rm("/FileStore/tables/subscription_plan_id.csv")

# COMMAND ----------

def get_data_explorer_authentication_dict(resource_name):
  """
  Gets all parameters needed to authenticate with known data explorer resources. Can be used to retrieve data from data explorer.
  
  :param resource_name: name of the data explorer resource, for example uelprdadx
  :type resource_name: str
  """
  supported_resources = ['uelprdadx', 'ueldevadx']
  
  if resource_name not in supported_resources:
    dbutils.notebook.exit("Enter a valid data explorer resource group!")
    
  if resource_name == 'uelprdadx':
    cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
    client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" 
    client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" 
    authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID
    authentication_dict = {
      'cluster': cluster,
      'client_id': client_id,
      'client_secret': client_secret,
      'authority_id':authority_id
    }
    
  elif resource_name == 'ueldevadx':
    cluster = "https://ueldevadx.westeurope.kusto.windows.net" # id 
    client_id = "8637a5e9-864d-4bd9-bb2e-be8ce991dbf6" # vervangen
    client_secret = "OJ.vcZo42y8_Kr5tsAXc6ImK~5b~s9k~0n" # 
    authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID (Vervangen)
    authentication_dict = {
      'cluster': cluster,
      'client_id': client_id,
      'client_secret': client_secret,
      'authority_id':authority_id
    }
  return authentication_dict

# COMMAND ----------

def run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'uel', df_format = 'dfs'):
  """
  Authenticates with a given data explorer cluster, and gets data executing a query.
  
  :param query: adx query to run on the target database
  :param resource_name: name of the resource, for example uelprdadx
  :param kustoDatabase: the database in which to query on the selected resource_group
  :param df_format: description of the return type, dfs for lightweighted spark dataframes, df for more heavy weighted pandas dataframes.
  
  :type query: str
  :type resource_name: str
  :type kustoDatabase: str
  :type df_format: str
  """
  
  supported_df_format = ['dfs', 'df']
  if df_format not in supported_df_format:
    sys.exit("Dataframe format not supported!")
    
  authentication_dict = get_data_explorer_authentication_dict(resource_name)
  
  pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
  kustoOptions = {"kustoCluster":authentication_dict['cluster'] 
                   ,"kustoDatabase":kustoDatabase
                  ,"kustoAadAppId":authentication_dict['client_id'] 
                  ,"kustoAadAppSecret":authentication_dict['client_secret']
                  , "kustoAadAuthorityID":authentication_dict['authority_id']}

  df  = pyKusto.read. \
              format("com.microsoft.kusto.spark.datasource"). \
              option("kustoCluster", kustoOptions["kustoCluster"]). \
              option("kustoDatabase", kustoOptions["kustoDatabase"]). \
              option("kustoQuery", query). \
              option("kustoAadAppId", kustoOptions["kustoAadAppId"]). \
              option("kustoAadAppSecret", kustoOptions["kustoAadAppSecret"]). \
              option("kustoAadAuthorityID", kustoOptions["kustoAadAuthorityID"]). \
              load()
  
  if df_format == 'df':
    df = df.toPandas()
  
  return df



# COMMAND ----------

query = """
video_genre
| distinct id
"""

movie_list_current = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'reporting', df_format = 'dfs').toPandas()['id'].unique().tolist()

movie_id_lookup_query = """
SELECT 
id AS movie_id,
genres as genres,
Channel as channel
FROM static_tables.nfn_hor_tt_wl_video_details_csv
"""
dfs = spark.sql(movie_id_lookup_query)

movie_list = dfs.toPandas()["movie_id"].unique().tolist()

if len(movie_list) == len(movie_list_current):
  write = False
  dbutils.notebook.exit("Dimension table subscriptions is already complete!")
  
new_movie_list = set(movie_list) - set(movie_list_current)
if len(movie_list_current) == 0:
  write = True
else:
  dfs = dfs.filter(dfs['movie_id'].isin(new_movie_list))


# COMMAND ----------

cols = ['movie_id','genres','channel']
dfs = dfs.select(cols)
dfs = dfs.withColumn("Created", lit(str(datetime.now(pytz.timezone('Europe/Amsterdam')))))

# COMMAND ----------

def write_to_data_explorer(dfs, table_name, resource_name ='uelprdadx', kustoDatabase = 'uel', write = True):
  """
  Write a spark dataframe to a selected data explorer table. View the microsoft documentation on: https://docs.microsoft.com/en-us/azure/data-explorer/spark-connector.
  
  :param dfs: spark dataframe
  :param table_name: name of the table of the target destination
  :param resource_name: name of the resource group of the data explorer destination table
  :param kustoDatabase: the database in which to query on the selected resource_group
  :param write: whether to actually proceed writing to the target destination
  
  :type dfs: spark dataframe
  :type table_name: str
  :type resource_name: str
  :type kustoDatabase: str
  :type write: boolean
  """
  authentication_dict = get_data_explorer_authentication_dict(resource_name)

  pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
  kustoOptions = {"kustoCluster":authentication_dict['cluster'] 
                   ,"kustoDatabase":kustoDatabase
                  ,"kustoAadAppId":authentication_dict['client_id'] 
                  ,"kustoAadAppSecret":authentication_dict['client_secret']
                  , "kustoAadAuthorityID":authentication_dict['authority_id']}

  if write:
    dfs.write. \
      format("com.microsoft.kusto.spark.datasource"). \
      option("kustoCluster",kustoOptions["kustoCluster"]). \
      option("kustoDatabase",kustoOptions["kustoDatabase"]). \
      option("kustoTable", table_name). \
      option("kustoAadAppId",kustoOptions["kustoAadAppId"]). \
      option("kustoAadAppSecret",kustoOptions["kustoAadAppSecret"]). \
      option("kustoAadAuthorityID",kustoOptions["kustoAadAuthorityID"]). \
      option("tableCreateOptions","CreateIfNotExist"). \
      mode("append"). \
      save()

resource_name = 'uelprdadx'
kustoDatabase = 'reporting'

write_to_data_explorer(dfs, 'video_genre', resource_name, kustoDatabase, write)

# COMMAND ----------

nr_new_records = dfs.count()
print("Number of records loaded to destination: {}".format(nr_new_records))
dbutils.notebook.exit("success")

# COMMAND ----------


