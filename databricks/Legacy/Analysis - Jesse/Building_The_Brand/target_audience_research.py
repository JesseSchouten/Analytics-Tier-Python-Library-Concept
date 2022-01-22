# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest

# COMMAND ----------

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

import pandas as pd
import numpy as np
import itertools
from datetime import timedelta,datetime
import matplotlib.pyplot as plt
import seaborn as sns
from itertools import product
import math

# COMMAND ----------

#prd:
cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" #
client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" #
authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,client_id,client_secret, authority_id)

client = KustoClient(kcsb)

# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/pushnotifications_test2_userbase.csv")

def round_duration(x):
  #CHECK WITH ARON: WE SHOULD COUNT FROM 0 AFTER TRIAL STARTS
  if x >= 14:
    return math.ceil((x)/30)
  else:
    return 0
  
def to_quarters(x):
  if x == 0:
    return 0
  else:
    return math.ceil(x/3)

def get_adf_df():
  db ='playground'

  query_ADF = """
  subscriptions
  | project Date=Date, SubscriptionId = ['Subscription.Id'], CustomerId = ['Customer.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], SubscriptionPlanId = ['SubscriptionPlan.Id']
  | where Date >= startofday(datetime('2021-1-14')) and Date < startofday(datetime('2021-1-14') + 1d)
  | project SubscriptionId, CustomerId, SubscriptionCreatedDate, SubscriptionEndDate, SubscriptionPlanId;
  """
  response = client.execute(db, query_ADF)
  df_adf = dataframe_from_result_table(response.primary_results[0])
  
  return df_adf

def get_subscription_plan_df():
  query = """
  SELECT d1.subscription_id, d1.channel_country, d1.channel, d1.country, d1.contract_duration, d2.market
  FROM static_tables.subscription_plan_id as d1 
  left join static_tables.country_market as d2 
  on
  d1.country = d2.country;
  """
  dfs_subscription_plan_id = spark.sql(query)
  df_subscription_plan_id = dfs_subscription_plan_id.toPandas()
  
  return df_subscription_plan_id

def get_enriched_adf_df():
  df_adf = get_adf_df()
  df_subscription_plan_id = get_subscription_plan_df()
  df_adf_temp = df_adf[['CustomerId','SubscriptionCreatedDate']].groupby('CustomerId').max().reset_index()
  df_ADF_lastcreateddate = pd.merge(df_adf_temp,df_adf,how='left',left_on = ['CustomerId','SubscriptionCreatedDate'],right_on = ['CustomerId','SubscriptionCreatedDate'])

  #df_adf = pd.merge(df_adf,df_subscription_plan_id[['subscription_id', 'channel_country']],how='left',left_on = ['SubscriptionPlanId'],right_on = ['subscription_id'])

  df_ADF_lastcreateddate = pd.merge(df_ADF_lastcreateddate,df_subscription_plan_id[['subscription_id','channel_country','channel','country']],how='left',left_on = ['SubscriptionPlanId'],right_on = ['subscription_id'])

  platform_selection = ['WL NL', 'WL NO', 'WL SE','WL BE']
  df_ADF_lastcreateddate = df_ADF_lastcreateddate[df_ADF_lastcreateddate['channel_country'].isin(platform_selection)]

  return df_ADF_lastcreateddate

def get_watchtime_df(userid_list = []):
  n= 10000
  userid_list_batches = [l[i * n:(i + 1) * n] for i in range((len(l) + n - 1) // n )] 
  df_watchtime = pd.DataFrame()
  for batch in userid_list_batches:
    db = "uel"
    query_watchtime = """
    set truncationmaxrecords=10000000;
    tracking
    | where userId in {} and timestamp >= datetime('2020-12-1') and timestamp <= datetime('2021-1-14')
    | summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by timestamp=bin(timestamp + 1h, 1h), userId, videoId, platform;
    """.format(tuple(batch))
  
    df_content = get_content_df()
    response = client.execute(db, query_watchtime)
    df_watchtime = pd.concat((df_watchtime, dataframe_from_result_table(response.primary_results[0])))
  df_watchtime = pd.merge(df_watchtime,df_content,how='left',left_on ='videoId',right_on ='id_v1')
  
  df_watchtime['date'] = df_watchtime['timestamp'].apply(lambda x: x.date())
  df_watchtime['hour'] = df_watchtime['timestamp'].apply(lambda x: x.hour)
  df_watchtime = pd.merge(df_watchtime,df_ADF[['CustomerId','channel_country','SubscriptionCreatedDate','SubscriptionEndDate']],how='left',left_on='userId',right_on='CustomerId')

  df_watchtime['Duration'] = df_watchtime['timestamp'] - df_watchtime['SubscriptionCreatedDate'] + timedelta(1)
  df_watchtime['Duration'] = df_watchtime['Duration'].apply(lambda x: x.total_seconds() / (60*60*24))
  df_watchtime['Duration_month'] = df_watchtime['Duration'].apply(lambda x: round_duration(x))
  df_watchtime['Duration_quarter'] = df_watchtime['Duration_month'].apply(lambda x: to_quarters(x))
  
  print("Nr movies not found in export: {}".format(len(df_watchtime[df_watchtime['id_v1'].isna()]['videoId'].unique())))
  print("Has effect on {} out of {} records".format(len(df_watchtime[df_watchtime['id_v1'].isna()]),len(df_watchtime)))
  return df_watchtime

def get_content_df():
  query = """
  SELECT *
  FROM default.withlove_content
  """
  dfs_content = spark.sql(query)
  df_content = dfs_content.toPandas()
  df_movies = df_content[df_content['asset_type'] == 'Movie']
  df_series = df_content[df_content['asset_type'] != 'Movie']
  df_movies['id_v1'] = '1-0-' + df_movie['id'].apply(lambda x: str(x).lower())
  df_series['id_v1'] = '1-1-' + df_series['id'].apply(lambda x: str(x).lower())
  df_content = pd.concat((df_movies,df_series))
  return df_content[['id','id_v1','genres','age_rating','actors','directors']]

df_ADF =  get_enriched_adf_df()
df_content = get_content_df()
df_watchtime = get_watchtime_df(tuple(df_ADF['CustomerId'].unique().tolist()))


# COMMAND ----------

"""
CELL TO TEST STUFF.
"""

df_watchtime

# COMMAND ----------

def watchtime_splitter(df_watchtime,split_col = 'genres'):
  genre_columns = ['timestamp', 'userId', 'videoId', 'playtime', 'id', 'id_v1', 'genres',
         'age_rating', 'directors']
  actor_columns = ['timestamp', 'userId', 'videoId', 'playtime', 'id', 'id_v1',
         'age_rating', 'actors', 'directors']
  if split_col == 'genres':
    df = df_watchtime[genre_columns].applymap(lambda x: x.split(', ') if isinstance (x, str) else [x]) 
  elif split_col == 'actors':
    df = df_watchtime[actor_columns].applymap(lambda x: x.split(', ') if isinstance (x, str) else [x]) 
  else:
    sys.exit('split col not supported!')
  df = pd.DataFrame([j for i in df.values for j in product(*i)], columns=df.columns)
  
  df = df[[split_col,'playtime']].groupby(split_col).sum().reset_index().sort_values(by='playtime',ascending=False)
  return df

display(watchtime_splitter(df_watchtime,split_col = 'genres'))

# COMMAND ----------

display(df_watchtime[['date','platform', 'playtime']].groupby(['date','platform']).sum().reset_index())

# COMMAND ----------

t = df_watchtime[['date','hour','channel_country', 'playtime']].groupby(['date','hour','channel_country']).sum().reset_index()
display(t[t['date'] > datetime(2021,1,1,0,0,0).date()])

# COMMAND ----------


