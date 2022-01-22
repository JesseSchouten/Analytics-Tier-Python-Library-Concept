# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib
# MAGIC %pip install aiohttp

# COMMAND ----------

import sys
import gc
import requests
import argparse 
import os
import ast 

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType, StructType, StructField
from pyspark.sql.functions import udf, array, lit, col

import pandas as pd
import numpy as np
from datetime import datetime,timedelta

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

current_date = "2021-4-20"
subscription_data_query = """
subscriptions
| where startofday(datetime({})) == startofday(Date)
| project customer_id = ['Customer.Id'], subscription_id = ['Subscription.Id'], subscription_created_date = ['Subscription.CreatedDate'], subscription_end_date = ['Subscription.SubscriptionEnd']
""".format(current_date)
subscription_df = run_data_explorer_query(subscription_data_query, kustoDatabase = 'playground').toPandas()
subscription_df = subscription_df[subscription_df['subscription_end_date'] >= current_date]
current_user_list = subscription_df['customer_id'].unique().tolist()


query = """
SELECT axinom_id, subscription_created_date,platform FROM currentsubs_nomail GROUP BY axinom_id, subscription_created_date,platform
"""
df = spark.sql(query).toPandas()
user_id_nfnnl = df[df['platform'] == 'NFN NL']['axinom_id'].unique().tolist()
user_id_nfnau = df[df['platform'] == 'NFN AU']['axinom_id'].unique().tolist()
user_id_nfnno = df[df['platform'] == 'NFN NO']['axinom_id'].unique().tolist()
user_id_wlnl = df[df['platform'] == 'WL NL']['axinom_id'].unique().tolist()

print("Nr of NFN NL users before ex customer exclusion: {}".format(len(user_id_nfnnl)))
print("Nr of NFN AU users before ex customer exclusion: {}".format(len(user_id_nfnau)))
print("Nr of NFN NO users before ex customer exclusion: {}".format(len(user_id_nfnno)))
print("Nr of WL NL users before ex customer exclusion: {}".format(len(user_id_wlnl)))

df = df[df['axinom_id'].isin(current_user_list)]

user_id_nfnnl = df[df['platform'] == 'NFN NL']['axinom_id'].unique().tolist()
user_id_nfnau = df[df['platform'] == 'NFN AU']['axinom_id'].unique().tolist()
user_id_nfnno = df[df['platform'] == 'NFN NO']['axinom_id'].unique().tolist()
user_id_wlnl = df[df['platform'] == 'WL NL']['axinom_id'].unique().tolist()

print("Nr of NFN NL users after ex customer exclusion: {}".format(len(user_id_nfnnl)))
print("Nr of NFN AU users after ex customer exclusion: {}".format(len(user_id_nfnau)))
print("Nr of NFN NO users after ex customer exclusion: {}".format(len(user_id_nfnno)))
print("Nr of WL NL users after ex customer exclusion: {}".format(len(user_id_wlnl)))

df['group'] = None
df[0:10]

# COMMAND ----------




# COMMAND ----------

channel_id_dict = {
    'nfn': 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d',
    'hor': '0ffffbf3-1d42-55f3-82c0-cb5029f55003',
    'tt': '25b748df-e99f-5ec9-aea5-967a73c88381',
    'wl': 'a495e56a-0139-58f3-9771-cec7690df76e'
}

channel = 'wl'
channel_id = channel_id_dict[channel]

viewing_data_query = """
set truncationmaxsize=1073741824;
set truncationmaxrecords=500000000000;
tracking
| where videoPlayTime >=0 and isTest == false and startofday(timestamp) >= startofday(datetime({})) - 57d and startofday(timestamp) <= startofday(datetime({})) - 1d and channel == '{}'
| project timestamp, subscription_id = subscriptionId, user_id = userId, video_id = videoId, video_type = videoType, platform = platform, video_playtime = videoPlayTime, ip_address = ipAddress, channel = channel
| summarize video_playtime_minutes = round(toreal(sum(video_playtime)) / 60)  by timestamp = bin(AdjustTimezone(timestamp,0), 1d), user_id
| project startofday(timestamp),user_id ,video_playtime_minutes
""".format(current_date,current_date,channel_id)
print(viewing_data_query)

viewing_df = run_data_explorer_query(viewing_data_query, kustoDatabase = 'uel').toPandas()

date_list = date_list = viewing_df['timestamp'].unique().tolist()

# COMMAND ----------

viewing_df_nfnnl = viewing_df[viewing_df['user_id'].isin(user_id_nfnnl)]
viewing_df_nfnau = viewing_df[viewing_df['user_id'].isin(user_id_nfnau)]
viewing_df_nfnno = viewing_df[viewing_df['user_id'].isin(user_id_nfnno)]
viewing_df_wlnl = viewing_df[viewing_df['user_id'].isin(user_id_wlnl)]

selected_viewing_df = viewing_df_wlnl.copy()
user_id_list = user_id_wlnl

# COMMAND ----------

#Sample from group, create NUMBER_OF_SAMPLE_GROUPS groups

import argparse 
import ast
import json
import sys

import random
import pandas as pd

random.seed(9)

def sample_without_replacement(user_list,sample_size):
    return random.sample(user_list,sample_size)

def create_experimental_groups(user_list, control_group_size = 2000):
    control_group_user_list = sample_without_replacement(user_list,control_group_size)
    experimental_group_user_list = list(set(user_list) - set(control_group_user_list))
    
    return control_group_user_list, experimental_group_user_list
control_group_list,experimental_group_list = create_experimental_groups(user_id_list)

print("Start: {}".format(len(selected_viewing_df)))
mux = pd.MultiIndex.from_product([pd.DatetimeIndex(date_list), 
                                  user_id_list], names=['timestamp','user_id'])

selected_viewing_df['timestamp'] = pd.to_datetime(selected_viewing_df['timestamp'])
selected_viewing_df = selected_viewing_df.set_index(['timestamp','user_id']).reindex(mux, fill_value=0)
selected_viewing_df = selected_viewing_df.reset_index()

print("End: {}".format(len(selected_viewing_df)))


# COMMAND ----------

"""
Measurements:

VIEWING TIME
- Nr of daily viewing time per user.
- Nr of viewing moments per week per user.
   - for all users
   - for users with at least 1 minute of viewing time in the past 8 weeks.

ENGAGEMENT STATUS
- % of dead users; no viewing time in past 8 weeks.
- % of idle users (no viewing time in past 4 weeks, at least 1 min in 4 weeks before that).
- (% of users with at least 1 minute viewing time in past 8 weeks).
"""

# COMMAND ----------


def add_column_avg_viewing_time_past_8_weeks(viewing_df, current_date = datetime(2021,4,20,0,0,0), past_days = 56):
  start_date = current_date - timedelta(days=past_days)
  
  current_date= pd.to_datetime(current_date.date())
  start_date = pd.to_datetime(start_date.date())
  
  viewing_df = viewing_df[viewing_df['timestamp'] < current_date]
  viewing_df = viewing_df[ viewing_df['timestamp'] >= start_date]
  
  nr_dates = len(viewing_df['timestamp'].unique().tolist())
  
  viewing_df_grouped =viewing_df[['user_id','video_playtime_minutes']].groupby(['user_id']).sum().reset_index()
  viewing_df_grouped['video_playtime_minutes'] = (viewing_df_grouped['video_playtime_minutes'] / nr_dates) *7
  viewing_df_grouped = viewing_df_grouped[viewing_df_grouped['video_playtime_minutes'] > 0]
  
  print("Average: {}".format(viewing_df_grouped['video_playtime_minutes'].mean()))
  print("SD: {}".format(viewing_df_grouped['video_playtime_minutes'].std()))
  print("median: {}\n".format(viewing_df_grouped['video_playtime_minutes'].median()))
  
  return viewing_df_grouped

print("Control group:")
display(add_column_avg_viewing_time_past_8_weeks(selected_viewing_df[selected_viewing_df['user_id'].isin(control_group_list)]),current_date) 
print("Experimental group:")
display(add_column_avg_viewing_time_past_8_weeks(selected_viewing_df[selected_viewing_df['user_id'].isin(experimental_group_list)]),current_date)

# COMMAND ----------

def print_df_stats(df, col_name):
  print("Average: {}".format(df[col_name].mean()))
  print("SD: {}".format(df[col_name].std()))
  print("median: {}\n".format(df[col_name].median()))

def mark_viewingmoment(x):
  if x >0:
    return 1
  else:
    return 0
  
def plot_graphs(df, print_stats = True):
  viewing_df = df.copy()
  viewing_df['week'] = viewing_df['timestamp'].apply(lambda x: x.week)
  #viewing_df = viewing_df[viewing_df['video_playtime_minutes'] > 0]
  engagementstatus_df = viewing_df[['user_id','video_playtime_minutes']].groupby('user_id').sum()
  dead_user_list = engagementstatus_df[engagementstatus_df['video_playtime_minutes'] == 0].index.tolist()
  non_dead_user_list = engagementstatus_df[engagementstatus_df['video_playtime_minutes'] >= 0].index.tolist()
  print("filter out {} dead users from {}".format(len(dead_user_list),len(non_dead_user_list)))

  #filter incomplete weeks and dead users (no monitored viewing time)
  check_df = viewing_df[['week','timestamp']].groupby(['week']).nunique()
  valid_weeks = check_df[check_df['timestamp'] == 7].index.tolist()
  print('valid weeks: {}'.format(valid_weeks))
  viewing_df = viewing_df[viewing_df['week'].isin(valid_weeks)]
  viewing_df = viewing_df[~viewing_df['user_id'].isin(dead_user_list)]
  
  viewing_df = viewing_df[['week','user_id','video_playtime_minutes']].groupby(['week','user_id']).sum()
  viewing_df['viewing_moments'] = viewing_df['video_playtime_minutes'].apply(lambda x: mark_viewingmoment(x))
  viewing_df_grouped = viewing_df.reset_index()[['user_id','viewing_moments']].groupby('user_id').sum()
  
  nr_enthousiasts = len(viewing_df_grouped[viewing_df_grouped['viewing_moments'] >=7].reset_index()['user_id'].unique().tolist())
  print("Number of enthousiasts: {}".format(nr_enthousiasts))
  if print_stats:
    print("stats:")
    print_df_stats(viewing_df, 'video_playtime_minutes')

    display(viewing_df)
    #zero_cases = len(viewing_df[viewing_df['video_playtime_minutes'] == 0]) * 100 / len(viewing_df)
    #print("Total cases: {}".format(len(viewing_df)))
    #print("daily % people not watching: {}%\n".format(zero_cases))

print("control group:") 
plot_graphs(selected_viewing_df[selected_viewing_df['user_id'].isin(control_group_list)])
print("experimental group:") 
plot_graphs(selected_viewing_df[selected_viewing_df['user_id'].isin(experimental_group_list)])

# COMMAND ----------

def mark_viewingmoment(x):
  if x >0:
    return 1
  else:
    return 0
  
def mark_past_4_weeks(x,date):
  if x >= date - timedelta(days=28):
    return 1
  else: 
    return 0
  
def mark_active_user(x, active_user_list):
  if x in active_user_list:
    return 1
  else: 
    return 0
def mark_week(x):
  return x.week

def print_activity_stats(df):
#  current_date = "2021-4-19"
  viewing_df = df.copy()
  current_date = pd.to_datetime(datetime(2021,4,20,0,0,0).date())
  viewing_df['timestamp'] = viewing_df['timestamp'].apply(lambda x: pd.to_datetime(x))
  viewing_df['viewing_moment'] = viewing_df['video_playtime_minutes'].apply(lambda x: mark_viewingmoment(x))
  viewing_df['past_4_weeks'] = viewing_df['timestamp'].apply(lambda x: mark_past_4_weeks(x,current_date))
  engagementstatus_df = viewing_df[['user_id','viewing_moment']].groupby('user_id').sum()
  dead_user_list = engagementstatus_df[engagementstatus_df['viewing_moment'] == 0].index.tolist()
  active_user_list = engagementstatus_df[engagementstatus_df['viewing_moment'] > 0].index.tolist()
  
  viewing_df_grouped = viewing_df[['user_id','viewing_moment']].groupby('user_id').sum().reset_index()
  viewing_df_grouped['is_active'] = viewing_df_grouped['user_id'].apply(lambda x: mark_active_user(x,active_user_list))
  nr_dead_users = len(viewing_df_grouped[viewing_df_grouped['viewing_moment'] == 0])
  total_users = len(viewing_df_grouped['user_id'].unique().tolist())
  dead_user_perc = nr_dead_users * 100/ (total_users)
  print("Nr of users: {}".format(total_users))
  print("Nr of dead users: {}".format(nr_dead_users))
  print("% of dead users: {}%".format(dead_user_perc))

  viewing_df_grouped = viewing_df[['user_id','viewing_moment','past_4_weeks']].groupby(['user_id','past_4_weeks']).sum().reset_index()
  viewing_df_grouped_0 = viewing_df_grouped[viewing_df_grouped['past_4_weeks'] == 0]
  viewing_df_grouped_1 = viewing_df_grouped[viewing_df_grouped['past_4_weeks'] == 1]
  inactive_users_past4weeks = viewing_df_grouped_1[viewing_df_grouped_1['viewing_moment'] == 0]['user_id'].unique().tolist()
  active_users_over4weeksago = viewing_df_grouped_0[viewing_df_grouped_0['viewing_moment'] > 0]['user_id'].unique().tolist()
  nr_idle_users = len(list(set(inactive_users_past4weeks) & set(active_users_over4weeksago)))
  idle_user_perc = nr_idle_users * 100 / (total_users)
  print("Nr of idle users: {}".format(nr_idle_users))
  print("% of idle users: {}%".format(idle_user_perc))
  
  viewing_df['weeknr'] = viewing_df['timestamp'].apply(lambda x: mark_week(x))
  viewing_df = viewing_df.reset_index()[['user_id','viewing_moment']].groupby('user_id').mean()
  print('Average views per week of all customers: {}'.format(viewing_df.mean()[0]*7))
  print('Average views per week of all customers with at least 1 viewing moment: {}\n'.format(viewing_df[viewing_df.index.isin(active_user_list)].mean()[0]*7))
  
print("control group:") 
viewing_df = selected_viewing_df.copy()
viewing_df = viewing_df[viewing_df['timestamp'].isin(date_list)]
print_activity_stats(viewing_df[viewing_df['user_id'].isin(control_group_list)])
print("experimental group:") 
print_activity_stats(viewing_df[viewing_df['user_id'].isin(experimental_group_list)])

# COMMAND ----------



# COMMAND ----------

def mark_group(x1, x2, control_group_list, exp_group_list):
  if x1 in control_group_list:
    return 'control_group'
  elif x1 in exp_group_list:
    return 'experimental_group'
  else:
    return x2
  
df['group'] = df.apply(lambda x: mark_group(x[0],x[3],control_group_list, experimental_group_list),axis=1)
df

# COMMAND ----------

df[['axinom_id','group','platform']].groupby(['group','platform']).nunique()

# COMMAND ----------



df.to_csv('/dbfs/FileStore/df/currentsubs_groups.csv')

# COMMAND ----------


