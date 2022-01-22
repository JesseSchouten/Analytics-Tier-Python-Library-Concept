# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import udf, array, lit, col

import sys

import numpy as np
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import math

# COMMAND ----------

def get_data_explorer_authentication_dict(resource_name):
  """
  Gets all parameters needed to authenticate with known data explorer resources. Can be used to retrieve data from data explorer.
  
  :param resource_name: name of the data explorer resource, for example uelprdadx
  :type resource_name: str
  """
  supported_resources = ['uelprdadx']
  
  if resource_name not in supported_resources:
    sys.exit("Enter a valid data explorer resource group!")
    
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
  return authentication_dict

# COMMAND ----------

def run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'uel', df_format = 'dfs'):
  """
  Authenticates with a given data explorer cluster, and gets data executing a query.
  
  :param query:
  :param resource_name:
  :param kustoDatabase:
  :param df_format:
  
  :type query:
  :type resource_name:
  :type kustoDatabase:
  :type df_format:
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
SELECT subscription_id, channel, country, contract_duration, created as date
FROM static_tables.subscription_plan_id 
"""
dfs_subscription_plan_id = spark.sql(query)
del query
dfs_subscription_plan_id.show(10)

# COMMAND ----------

def to_date(x):
  x = str(x)
  try:
    return datetime.strptime(x, '%Y-%m-%d')
  except ValueError:
    try:
      return datetime.strptime(x, '%m/%d/%Y')
    except ValueError:
      return datetime(1900,1,1,0,0,0)
  return 0

if dfs_subscription_plan_id.select('date').dtypes[0][1] != 'timestamp':
  Created = udf(to_date, TimestampType())
  dfs_subscription_plan_id = dfs_subscription_plan_id.withColumn("date", Created("date"))

# COMMAND ----------

query = """
SubscriptionPlan
| summarize LastDate = max(Date)
"""
last_created_date = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'shared', df_format = 'dfs').select('LastDate').first()[0]
if type(last_created_date) == type(None):
  write = True
else: 
  dfs_subscription_plan_id = dfs_subscription_plan_id.filter(dfs_subscription_plan_id['Date']>last_created_date)
  write = True

if dfs_subscription_plan_id.count() == 0:
  write = False
  sys.exit("TERMINATE - No new data to load.")
dfs_subscription_plan_id.show(1000)

# COMMAND ----------

cols = ['date', 'subscription_id', 'channel','country', 'contract_duration']
dfs_final = dfs_subscription_plan_id.select(cols)
dfs_final = dfs_final.withColumn("Created", lit(datetime.now()))


# COMMAND ----------

def write_to_data_explorer(dfs, table_name, resource_name ='uelprdadx', kustoDatabase = 'uel', write = True):
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
kustoDatabase = 'shared'
write_to_data_explorer(dfs_final, 'SubscriptionPlan', resource_name, kustoDatabase, write)

# COMMAND ----------


