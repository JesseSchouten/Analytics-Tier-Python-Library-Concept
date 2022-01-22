# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.functions import udf, array, lit, col, min, max

import sys
import os

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

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
viewing
| summarize max_date = max(startofday(timestamp))
"""

last_update_datetime = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'reporting', df_format = 'dfs').select('max_date').first()[0]

today_datetime = datetime.now()
time_selection = ''
second_time_selection = ''
destination_table_is_empty = False
write = True
hard_coded_start_date = '2020-12-16'
if type(last_update_datetime) == type(None):
  destination_table_is_empty = True
  time_selection =  "and timestamp < startofday(datetime({})) ".format(today_datetime.date())
  time_selection += "and timestamp >= startofday(datetime({}))".format(hard_coded_start_date)
  second_time_selection = "and timestamp < startofday(datetime({}))".format(hard_coded_start_date)
elif last_update_datetime.date() < today_datetime.date() - timedelta(days=1):
  time_selection = "and timestamp < startofday(datetime({})) and timestamp >= startofday(datetime({}) + 1d)".format(today_datetime.date(), last_update_datetime.date())
else:
  write = False
  dbutils.notebook.exit("Dataset was already refreshed!")

# COMMAND ----------

def clean_video_ids(x):
  """
  Remove 1-0 or 0-0 from the video ID formatting. 
  
  :param x: video_id from tracking table in data explorer
  :type x: str
  """
  return x[4:]

viewing_data_query = """
set truncationmaxsize=1073741824;
set truncationmaxrecords=500000000000;
  tracking
| where videoPlayTime >=0 and isTest == false {}
| project timestamp, user_id = userId, video_id = videoId, video_type = videoType, platform = platform, video_playtime = videoPlayTime, ip_address = ipAddress, channel = channel
| summarize video_playtime_minutes = round(toreal(sum(video_playtime)) / 60)  by timestamp = bin(AdjustTimezone(timestamp,0), 1h), user_id, video_id, video_type, platform, ip_address, channel
| join kind = leftouter 
        ( tracking
        | where videoPlayTime >= 0 and isTest == false and action in ("Pause")
        | project timestamp, user_id = userId,video_id = videoId, video_type= videoType, platform =platform, action = action, ip_address = ipAddress, channel
        | summarize interactions = count() by timestamp = bin(AdjustTimezone(timestamp,0), 1h), user_id , action,video_id, video_type, platform, ip_address, channel
        | evaluate pivot(action,sum(interactions))
        | project timestamp,user_id, video_id, video_type, ip_address, platform,channel, video_pauses = Pause
        ) on timestamp, user_id, video_id, video_type, ip_address, platform, channel
| project timestamp_date = startofday(timestamp), timestamp,user_id, video_id, video_type, platform, ip_address,channel, video_playtime_minutes, video_pauses
| join kind=leftouter   
        ( database("playground").subscriptions
        | project actual_date = startofday(Date - 1d), customer_id = ['Customer.Id'], subscription_created_date = ['Subscription.CreatedDate'],channel = Channel
        | summarize subscription_created_date = max(subscription_created_date) by actual_date, customer_id, channel
        | join kind=leftouter
            (
            database("playground").subscriptions
            | project actual_date = startofday(Date - 1d), customer_id = ['Customer.Id'], subscription_created_date = ['Subscription.CreatedDate'], subscription_plan_id = ['SubscriptionPlan.Id'],channel = Channel
            ) on $left.actual_date == $right.actual_date and $left.customer_id == $right.customer_id and $left.channel == $right.channel and $left.subscription_created_date == $right.subscription_created_date
        | project subscription_created_date,actual_date, customer_id, subscription_plan_id, channel
        | distinct subscription_created_date,actual_date, customer_id, subscription_plan_id, channel
        ) on $left.timestamp_date == $right.actual_date and $left.user_id == $right.customer_id and $left.channel == $right.channel
| project timestamp, user_id,subscription_created_date, subscription_plan_id, video_id, video_type, platform, ip_address, video_playtime_minutes, video_pauses
""".format(time_selection)

#viewing_data_query should be retrieved in daily batches!
dfs = run_data_explorer_query(viewing_data_query, resource_name ='uelprdadx', kustoDatabase = 'uel', df_format = 'dfs')

MovieID = udf(lambda x: clean_video_ids(x), StringType())
dfs = dfs.withColumn("video_id", MovieID("video_id"))

# COMMAND ----------

"""
Impute the subscription_plan_ids of viewing time data from before the subscription table was filled (before december 14th 2020).

This only has to be done once, if we load the entire table.
"""
viewing_data_query = """
set truncationmaxsize=1073741824;
set truncationmaxrecords=500000000000;
tracking
| where videoPlayTime >=0 and isTest == false {}
| project timestamp, user_id = userId, video_id = videoId, video_type = videoType, platform = platform, video_playtime = videoPlayTime, ip_address = ipAddress, channel = channel
| summarize video_playtime_minutes = round(toreal(sum(video_playtime)) / 60)  by timestamp = bin(AdjustTimezone(timestamp,0), 1h), user_id, video_id, video_type, platform, ip_address, channel
| join kind = leftouter 
        ( tracking
        | where videoPlayTime >= 0 and isTest == false and action in ("Pause")
        | project timestamp, user_id = userId,video_id = videoId, video_type= videoType, platform =platform, action = action, ip_address = ipAddress, channel
        | summarize interactions = count() by timestamp = bin(AdjustTimezone(timestamp,0), 1h), user_id , action,video_id, video_type, platform, ip_address, channel
        | evaluate pivot(action,sum(interactions))
        | project timestamp,user_id, video_id, video_type, ip_address, platform,channel, video_pauses = Pause
        ) on timestamp, user_id, video_id, video_type, ip_address, platform, channel
| project timestamp_date = startofday(timestamp), timestamp,user_id, video_id, video_type, platform, ip_address,channel, video_playtime_minutes, video_pauses
| join kind=leftouter   
        ( database("playground").subscriptions
        | where startofday(Date) == datetime({})
        | project customer_id = ['Customer.Id'], subscription_created_date = ['Subscription.CreatedDate'],channel = Channel
        | summarize subscription_created_date = max(subscription_created_date) by customer_id, channel
        | join kind=leftouter
            (
            database("playground").subscriptions
            | where startofday(Date) == datetime({})
            | project customer_id = ['Customer.Id'], subscription_created_date = ['Subscription.CreatedDate'], subscription_plan_id = ['SubscriptionPlan.Id'],channel = Channel
            ) on $left.customer_id == $right.customer_id and $left.channel == $right.channel and $left.subscription_created_date == $right.subscription_created_date
        | project subscription_created_date,customer_id, subscription_plan_id, channel
        | distinct subscription_created_date, customer_id, subscription_plan_id, channel
        ) on $left.user_id == $right.customer_id and $left.channel == $right.channel
| project timestamp, user_id,subscription_created_date, subscription_plan_id, video_id, video_type, platform, ip_address, video_playtime_minutes, video_pauses
""".format(second_time_selection, hard_coded_start_date, hard_coded_start_date)

if destination_table_is_empty:
  #viewing_data_query should be retrieved in daily batches!
  dfs_2 = run_data_explorer_query(viewing_data_query, resource_name ='uelprdadx', kustoDatabase = 'uel', df_format = 'dfs')

  MovieID = udf(lambda x: clean_video_ids(x), StringType())
  dfs_2 = dfs_2.withColumn("video_id", MovieID("video_id"))
  
  dfs = dfs.union(dfs_2)

# COMMAND ----------

cols = ['timestamp', 'user_id','subscription_created_date','subscription_plan_id', 'video_id','video_type','platform', 'ip_address','video_playtime_minutes', 'video_pauses']
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

write_to_data_explorer(dfs, 'viewing', resource_name, kustoDatabase, write)

# COMMAND ----------

nr_new_records = dfs.count()
print("Number of records loaded to destination: {}".format(nr_new_records))
dbutils.notebook.exit("success")
