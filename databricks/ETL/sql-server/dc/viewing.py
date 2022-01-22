# Databricks notebook source
# MAGIC %sh    
# MAGIC 
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

# https://docs.databricks.com/data/data-sources/sql-databases.html
# Python standard library
import sys
import os
import requests
import json
from datetime import datetime, timedelta
import time
import re
from urllib.parse import urlparse, quote_plus, quote

# Python open source libraries
import pandas as pd
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructType
from pyspark.sql.functions import udf, array, lit, col, min, max
import sqlalchemy as db

# Python DutchChannels libraries
from dc_azure_toolkit import adx, sql
from data_sql.main import main as data_sql_main
from data_sql.lib.SQLAuthenticator import SQLAuthenticator
from data_sql.lib.SQLConnector import SQLConnector

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

viewing_data_query = """
set truncationmaxsize=1073741824;
set truncationmaxrecords=500000000000;
let Channels = dynamic({{'nfn': 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d',
  'vg': '25b748df-e99f-5ec9-aea5-967a73c88381',
  'lov': 'a495e56a-0139-58f3-9771-cec7690df76e'
}});
playtime
| where videoEvent_videoPlayTime >=0 and startofday(timestamp) == startofday(datetime({}))
| project timestamp, subscription_id = videoEvent_sessionContext_subscriptionId, user_id = videoEvent_sessionContext_userId, video_id = videoEvent_videoId, video_type = videoEvent_videoType, platform = videoEvent_sessionContext_platform, video_playtime = videoEvent_videoPlayTime, channel = tostring(Channels[channel]) 
| summarize video_playtime_minutes = round(toreal(sum(video_playtime)) / 60)  by timestamp = bin(timestamp, 1h),subscription_id, user_id, video_id, video_type, platform, channel
| project timestamp, user_id, subscription_id, video_id, video_type, platform, channel, video_playtime_minutes
| where video_playtime_minutes > 0
"""

# COMMAND ----------

class SubscriptionPlanIdMapperImporter(SQLConnector):
  def __init__(self, context):
    super().__init__(context)
    
  def get_select_query(self, date):
    """
     Add the subscription_plan_id to the customer_id and latest subscription_created date found in the data.
     This avoids double counting of playtimes for subscribers who have had multiple subscriptions in the past.
    """
    
    return """
    SELECT t1.customer_id, t1.subscription_plan_id, t1.subscription_created_date, t1.channel
      FROM subscriptions_viewing_time_report AS t1 
    RIGHT JOIN (
      SELECT customer_id, channel, subscription_created_date = max(subscription_created_date)
        FROM subscriptions_viewing_time_report
      WHERE subscription_created_date <= '{}'
      GROUP BY customer_id, channel
    ) AS t2 ON t1.customer_id = t2.customer_id AND t1.subscription_created_date = t2.subscription_created_date
    WHERE t1.subscription_created_date <= '{}'
    """.format(date, date)
  
  def execute(self,date):
    self.open_connection('odbc')
    
    authentication_dict = SQLAuthenticator('data-prd-sql', dbutils=dbutils)
    
    result = sql.select(self.get_select_query(date), authentication_dict.get_authentication_dict(), df_format = 'df')
    
    return result


# COMMAND ----------

def get_date_range_list(start_date, end_date):
  result = []
  current_date = start_date
  
  while current_date != end_date:
    result.append(current_date)
    current_date = current_date + timedelta(days=1)
  result.append(current_date) 
  return result

authentication_dict_adx = ADXAuthenticator('uelprdadx').get_authentication_dict()

date_list = get_date_range_list(datetime.now().date() + timedelta(days = -1), datetime.now().date() + timedelta(days = -1))

insert_passed_list = []
for date in date_list:
  print("Start: {} at {}".format(date, str(datetime.now())))
  
  ctx = data_sql_main(environment='prd', dbutils=dbutils)
  
  df = adx.select(viewing_data_query.format(date), authentication_dict_adx, kustoDatabase = 'uds', df_format ='df') 
  
  df_mapper = SubscriptionPlanIdMapperImporter(ctx).execute(date)

  df_merge = pd.merge(df, df_mapper, how='left',left_on =['user_id', 'channel'], right_on =['customer_id', 'channel'])
  
  df_merge = df_merge[['timestamp', 'channel', 'subscription_plan_id','video_id', 'video_type', 'platform','video_playtime_minutes']]

  viewing_service = ctx['ViewingService']

  insert_passed = viewing_service.run_insert(df_merge, batch_size=10000)
  insert_passed_list.append(insert_passed)
  
  time.sleep(5)

# COMMAND ----------

if False in insert_passed_list:
  sys.exit("Error, viewing data is not inserted!")
