# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib
# MAGIC %pip install aiohttp
# MAGIC %pip install scipy
# MAGIC %pip install openpyxl

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

current_date = "2021-8-23"
start_date = '2021-6-28'
subscription_data_query = """
subscriptions
| where startofday(datetime({})) == startofday(Date)
| project customer_id = ['Customer.Id'], subscription_id = ['Subscription.Id'], subscription_created_date = ['Subscription.CreatedDate'], subscription_end_date = ['Subscription.SubscriptionEnd']
""".format(current_date)
subscription_df = run_data_explorer_query(subscription_data_query, kustoDatabase = 'playground').toPandas()
subscription_df = subscription_df[subscription_df['subscription_end_date'] >= start_date]
# subscription_df = subscription_df[subscription_df['subscription_end_date'] >= current_date]
# current_user_list = subscription_df['customer_id'].unique().tolist()

# query = """
# SELECT axinom_id, subscription_created_date,platform FROM currentsubs_nomail GROUP BY axinom_id, subscription_created_date,platform
# """

query = """
SELECT axinom_id, subscription_created_date, platform, group FROM SAMPLES_PROMOTED_FB_20210421
"""

df = spark.sql(query).toPandas()

ONLY_RECENT_USERS = False
if ONLY_RECENT_USERS:
  print("EXCLUDE USERS WITH CREATED_DATE < 2021-1-1")
  recent_user_df = subscription_df[subscription_df['subscription_created_date']>= datetime(2021,1,1,0,0,0)][['customer_id','subscription_created_date']]
  recent_user_df['subscription_created_date'] = recent_user_df['subscription_created_date'].apply(lambda x: datetime(x.year, x.month, x.day, 0, 0, 0))
  df = pd.merge(df, recent_user_df, left_on = ['axinom_id', 'subscription_created_date'], right_on = ['customer_id', 'subscription_created_date'], how = 'inner')

#user_id_nfnnl = df[df['platform'] == 'NFN NL']['axinom_id'].unique().tolist()
# user_id_nfnau = df[df['platform'] == 'NFN AU']['axinom_id'].unique().tolist()
# user_id_nfnno = df[df['platform'] == 'NFN NO']['axinom_id'].unique().tolist()
user_id_wlnl = df[df['platform'] == 'WL NL']['axinom_id'].unique().tolist()

#print("Nr of NFN NL users before ex customer exclusion: {}".format(len(user_id_nfnnl)))
# print("Nr of NFN AU users before ex customer exclusion: {}".format(len(user_id_nfnau)))
# print("Nr of NFN NO users before ex customer exclusion: {}".format(len(user_id_nfnno)))
print("Nr of WL NL users before ex customer exclusion: {}".format(len(user_id_wlnl)))

#df = df[df['axinom_id'].isin(current_user_list)]

#user_id_nfnnl = df[df['platform'] == 'NFN NL']['axinom_id'].unique().tolist()
#user_id_nfnau = df[df['platform'] == 'NFN AU']['axinom_id'].unique().tolist()
#user_id_nfnno = df[df['platform'] == 'NFN NO']['axinom_id'].unique().tolist()
#user_id_wlnl = df[df['platform'] == 'WL NL']['axinom_id'].unique().tolist()

#print("Nr of NFN NL users after ex customer exclusion: {}".format(len(user_id_nfnnl)))
#print("Nr of NFN AU users after ex customer exclusion: {}".format(len(user_id_nfnau)))
#print("Nr of NFN NO users after ex customer exclusion: {}".format(len(user_id_nfnno)))
#print("Nr of WL NL users after ex customer exclusion: {}".format(len(user_id_wlnl)))

df[0:10]

# COMMAND ----------

df[['platform','axinom_id','group']].groupby(['platform','group']).nunique()

# COMMAND ----------

channel = 'lov'

viewing_data_query = """
set truncationmaxsize=1073741824;
set truncationmaxrecords=500000000000;
playtime
| where videoEvent_videoPlayTime >=0 and startofday(timestamp) >= startofday(datetime('2021-6-28')) and startofday(timestamp) <= startofday(datetime('2021-8-23')) - 1d and channel == '{}'
| project timestamp, subscription_id = videoEvent_sessionContext_subscriptionId, user_id = videoEvent_sessionContext_userId, video_id = videoEvent_videoId, video_type = videoEvent_videoType, platform = videoEvent_sessionContext_platform, video_playtime = videoEvent_videoPlayTime, ip_address = ipAddress, channel = channel
| summarize video_playtime_minutes = round(toreal(sum(video_playtime)) / 60)  by timestamp = bin(AdjustTimezone(timestamp,0), 1d), user_id
| project startofday(timestamp),user_id ,video_playtime_minutes
""".format(channel)
print(viewing_data_query)

viewing_df = run_data_explorer_query(viewing_data_query, kustoDatabase = 'uds').toPandas()

date_list = date_list = viewing_df['timestamp'].unique().tolist()

# COMMAND ----------

#viewing_df_nfnnl = viewing_df[viewing_df['user_id'].isin(user_id_nfnnl)]
# viewing_df_nfnau = viewing_df[viewing_df['user_id'].isin(user_id_nfnau)]
# viewing_df_nfnno = viewing_df[viewing_df['user_id'].isin(user_id_nfnno)]
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

# random.seed(9)

# def sample_without_replacement(user_list,sample_size):
#     return random.sample(user_list,sample_size)

# def create_experimental_groups(user_list, control_group_size = 2000):
#     control_group_user_list = sample_without_replacement(user_list,control_group_size)
#     experimental_group_user_list = list(set(user_list) - set(control_group_user_list))
    
#     return control_group_user_list, experimental_group_user_list
# control_group_list,experimental_group_list = create_experimental_groups(user_id_list)

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

from scipy import stats
import scipy

def process_sub_df(subscription_df, start_date, current_date):
  subscription_df['last_possible_viewing_date'] = subscription_df['subscription_end_date'].apply(lambda x: current_date if x >= current_date else x)
  subscription_df['nr_last_possible_viewing_days'] = subscription_df['last_possible_viewing_date'] - start_date
  subscription_df['nr_last_possible_viewing_days'] = subscription_df['nr_last_possible_viewing_days'].apply(lambda x: x.days)
  subscription_df['nr_last_possible_viewing_days'] = subscription_df['nr_last_possible_viewing_days'].apply(lambda x: 0 if x <= 0 else x)
  subscription_df = subscription_df[['customer_id', 'subscription_end_date','nr_last_possible_viewing_days']].groupby(['customer_id']).max().reset_index()
  return subscription_df

def get_grouped_df(viewing_df, current_date = datetime(2021,4,20,0,0,0), past_days = 56):
  global subscription_df
  start_date = current_date - timedelta(days=past_days)
  print(start_date)
  
  current_date= pd.to_datetime(current_date.date())
  start_date = pd.to_datetime(start_date.date())
  viewing_df = viewing_df[viewing_df['timestamp'] < current_date]
  viewing_df = viewing_df[ viewing_df['timestamp'] >= start_date]
  nr_dates = len(viewing_df['timestamp'].unique().tolist())
  #take into account how many days someone could possibly have watched, based on the subscription end date. Use this to calculate weekly average
  viewing_df_grouped =viewing_df[['user_id','group','video_playtime_minutes']].groupby(['user_id','group']).sum().reset_index()
  subscription_df_processed = process_sub_df(subscription_df, start_date, current_date) 
  viewing_df_grouped = pd.merge(subscription_df_processed[['customer_id','nr_last_possible_viewing_days','subscription_end_date']],viewing_df_grouped,left_on='customer_id',right_on='user_id',how='right')
  viewing_df_grouped = viewing_df_grouped[['user_id','group','video_playtime_minutes','nr_last_possible_viewing_days']]

  viewing_df_grouped = viewing_df_grouped[viewing_df_grouped['nr_last_possible_viewing_days'] > 0]
  viewing_df_grouped['video_playtime_minutes'] = (viewing_df_grouped['video_playtime_minutes'] / nr_dates) * 7
  del viewing_df_grouped['nr_last_possible_viewing_days']
  return viewing_df_grouped

def get_outlier_list(viewing_df_grouped, current_date = datetime(2021,4,20,0,0,0), past_days = 56, nr_sd = 3):
  mean = viewing_df_grouped['video_playtime_minutes'].mean()
  sd = viewing_df_grouped['video_playtime_minutes'].std()
  viewing_df_grouped = viewing_df_grouped[(viewing_df_grouped['video_playtime_minutes'] > mean + nr_sd * sd)]
  outlier_list = viewing_df_grouped['user_id'].unique().tolist()
  print("{} outliers removed".format(len(outlier_list)))
  cg_count = len(df[(df['axinom_id'].isin(outlier_list)) & (df['group']=='control_group')]['axinom_id'].unique().tolist())
  exp_count = len(df[(df['axinom_id'].isin(outlier_list)) & (df['group']=='experimental_group')]['axinom_id'].unique().tolist())
  print("{} outliers in control group".format(cg_count))
  print("{} outliers in experimental group".format(exp_count))
  
  return outlier_list

def print_results(viewing_df_grouped, current_date = datetime(2021,4,20,0,0,0), past_days = 56):
  print("Group size: {}".format(len(viewing_df_grouped['user_id'].unique().tolist())))
  nr_zero_users = len(viewing_df_grouped[viewing_df_grouped['video_playtime_minutes'] == 0]['user_id'].unique())
  nr_total_users = len(viewing_df_grouped['user_id'].unique())
  viewing_df_grouped = viewing_df_grouped[(viewing_df_grouped['video_playtime_minutes'] > 0)]
  
  print("proportion users without viewing time: {}".format(nr_zero_users/nr_total_users))
  
  print("Average: {}".format(viewing_df_grouped['video_playtime_minutes'].mean()))
  print("SD: {}".format(viewing_df_grouped['video_playtime_minutes'].std()))
  print("median: {}\n".format(viewing_df_grouped['video_playtime_minutes'].median()))
  return viewing_df_grouped

current_date = datetime(2021,8,23,0,0,0)
past_days = 56

selected_viewing_df_2 = selected_viewing_df.merge(df[['axinom_id','group','platform']],how='left',left_on=['user_id'],right_on=['axinom_id'])
viewing_df_grouped = get_grouped_df(selected_viewing_df_2,current_date,past_days=past_days)

PROCESS_OUTLIERS = False
if PROCESS_OUTLIERS:
  nr_sd = 3
  outlier_list = get_outlier_list(viewing_df_grouped, current_date, past_days=past_days, nr_sd = nr_sd)
  mean = viewing_df_grouped['video_playtime_minutes'].mean()
  sd = viewing_df_grouped['video_playtime_minutes'].std()
  #viewing_df_grouped = viewing_df_grouped[~viewing_df_grouped['user_id'].isin(outlier_list)]
  viewing_df_grouped = viewing_df_grouped.set_index('user_id')
  for user_id in outlier_list:
    viewing_df_grouped.at[user_id, 'video_playtime_minutes'] = mean+nr_sd*sd
  viewing_df_grouped = viewing_df_grouped.reset_index()

print("Control group")
control_group_df = print_results(viewing_df_grouped[viewing_df_grouped['group']=='control_group'],current_date,past_days=past_days)
display(control_group_df)
print("Experimental group:")
exp_group_df = print_results(viewing_df_grouped[viewing_df_grouped['group']=='experimental_group'], current_date, past_days=past_days)
display(exp_group_df)

from scipy.stats import mannwhitneyu

test = 't-test'
if test == 't-test':
  print(stats.ttest_ind(control_group_df['video_playtime_minutes'], exp_group_df['video_playtime_minutes'], alternative='less'))
elif test == 'mann-whitney-u':
  U1, p = mannwhitneyu(control_group_df['video_playtime_minutes'], exp_group_df['video_playtime_minutes'], alternative='less')
  print("U1: {}".format(U1))
  print("p: {}".format(p))

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
  current_date = pd.to_datetime(datetime(2021,8,5,0,0,0).date())
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

print_activity_stats(selected_viewing_df_2[selected_viewing_df_2['group']=='control_group'])
print("experimental group:") 
print_activity_stats(selected_viewing_df_2[selected_viewing_df_2['group']=='experimental_group'])


# COMMAND ----------

#Churn Analysis

current_date = "2021-8-23"
experiment_start_date = "2021-8-5"  #21-04-2021
subscription_data_query = """
subscriptions
| where startofday(datetime({})) == startofday(Date)
| project customer_id = ['Customer.Id'], subscription_id = ['Subscription.Id'], subscription_created_date = ['Subscription.CreatedDate'], subscription_end_date = ['Subscription.SubscriptionEnd']
""".format(current_date)
subscription_df = run_data_explorer_query(subscription_data_query, kustoDatabase = 'playground').toPandas()

query = """
SELECT axinom_id, subscription_created_date, platform, group FROM SAMPLES_PROMOTED_FB_20210421
"""

df = spark.sql(query).toPandas()

ONLY_RECENT_USERS = False
if ONLY_RECENT_USERS:
  print("EXCLUDE USERS WITH CREATED_DATE < 2021-1-1")
  recent_user_df = subscription_df[subscription_df['subscription_created_date']>= datetime(2021,1,1,0,0,0)][['customer_id','subscription_created_date']]
  recent_user_df['subscription_created_date'] = recent_user_df['subscription_created_date'].apply(lambda x: datetime(x.year, x.month, x.day, 0, 0, 0))
  df = pd.merge(df, recent_user_df, left_on = ['axinom_id', 'subscription_created_date'], right_on = ['customer_id', 'subscription_created_date'], how = 'inner')[['axinom_id','subscription_created_date','platform','group']]
  
subscription_df = subscription_df[subscription_df['subscription_end_date'] >= experiment_start_date]
current_user_list = subscription_df['customer_id'].unique().tolist()
  
df = df[df['axinom_id'].isin(current_user_list)]
  
user_id_nfnnl = df[df['platform'] == 'NFN NL']['axinom_id'].unique().tolist()
# user_id_nfnau = df[df['platform'] == 'NFN AU']['axinom_id'].unique().tolist()
# user_id_nfnno = df[df['platform'] == 'NFN NO']['axinom_id'].unique().tolist()
# user_id_wlnl = df[df['platform'] == 'WL NL']['axinom_id'].unique().tolist()

print("Nr of NFN NL users before ex customer exclusion: {}".format(len(user_id_nfnnl)))
# print("Nr of NFN AU users before ex customer exclusion: {}".format(len(user_id_nfnau)))
# print("Nr of NFN NO users before ex customer exclusion: {}".format(len(user_id_nfnno)))
# print("Nr of WL NL users before ex customer exclusion: {}".format(len(user_id_wlnl)))

# COMMAND ----------

def missing_values_table(df):
    mis_val = df.isnull().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    mis_val_table = pd.concat([mis_val, mis_val_percent], axis=1)
    mis_val_table_ren_columns = mis_val_table.rename(
    columns = {0 : 'Missing Values', 1 : '% of Total Values'})
    mis_val_table_ren_columns = mis_val_table_ren_columns[
        mis_val_table_ren_columns.iloc[:,1] != 0].sort_values(
    '% of Total Values', ascending=False).round(1)
    print ("Your selected dataframe has " + str(df.shape[1]) + " columns.\n"      
        "There are " + str(mis_val_table_ren_columns.shape[0]) +
            " columns that have missing values.")
    return mis_val_table_ren_columns

# COMMAND ----------

#experiment start date : 21-04-2021

# rename columns
df.columns = ['customer_id', 'subscription_created_date','platform','group']

# remove old sub created date from experiment df
df = df.sort_values('subscription_created_date', ascending=False).drop_duplicates('customer_id').sort_index()

# remove time element
subscription_df['subscription_created_date'] = subscription_df['subscription_created_date'].apply(lambda x: str(x).split(' ')[0])

# change datatype to datetime
subscription_df['subscription_created_date']= pd.to_datetime(subscription_df['subscription_created_date'])
df['subscription_created_date']= pd.to_datetime(df['subscription_created_date'])

#change datatype to string
subscription_df['customer_id'] = subscription_df['customer_id'].astype('|S')
df['customer_id'] = df['customer_id'].astype('|S')

# decode customer ID
df['customer_id'] =df['customer_id'].apply(lambda x: x.decode('utf-8')) 
subscription_df['customer_id'] =subscription_df['customer_id'].apply(lambda x: x.decode('utf-8')) 

# join databases
merged_df = df.merge(subscription_df, on=['customer_id'], how='left')

# remove duplicates
final_df = merged_df.sort_values('subscription_created_date_y', ascending=False).drop_duplicates('customer_id').sort_index()

import numpy
# fill na for empty values
final_df['subscription_end_date'].replace('', np.nan, inplace=True)
# drop the na values
final_df.dropna(subset=['subscription_end_date'], inplace=True)

# remove secondary created date column and rename cols
del final_df['subscription_created_date_y']
final_df.columns = ['customer_id', 'subscription_created_date','platform','group', 'subscription_id', 'subscription_end_date']

# export to csv
#final_df.to_csv('/dbfs/FileStore/df/merged_sub.csv', index=False)

#check for missing values
missing_values_table(final_df)

# COMMAND ----------

# add churn measure based on today's date
current_date = datetime(2021,8,23,0,0,0).date()
final_df['churned'] = final_df['subscription_end_date'].apply(lambda x: x.date() <= current_date)

# COMMAND ----------

#final_df[['platform','customer_id','group']].groupby(['platform','group']).nunique()
#final_df[['platform','customer_id','group','churned']].groupby(['platform','group','churned']).sum()
churned_false_df = final_df[final_df['churned']==False][['customer_id','platform','group']].groupby(['platform','group']).nunique().reset_index()
churned_true_df = final_df[final_df['churned']==True][['customer_id','platform','group']].groupby(['platform','group']).nunique().reset_index()

churn_df = pd.merge(churned_false_df,churned_true_df,how='inner',left_on=['platform','group'],right_on=['platform','group']).rename(columns={'customer_id_x':'non churners','customer_id_y':'churners'})
churn_df['churn %'] = churn_df['churners']*100 / (churn_df['non churners'] + churn_df['churners'])
print(churn_df)
display(churn_df)

# COMMAND ----------

final_df.sort_values(by='subscription_created_date')
final_df['subscription_created_year'] = final_df['subscription_created_date'].dt.year
final_df['subscription_created_month'] = final_df['subscription_created_date'].dt.month
final_df['subscription_end_year'] = final_df['subscription_end_date'].dt.year
final_df['subscription_end_month'] = final_df['subscription_end_date'].dt.month

# COMMAND ----------

final_df

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.info()

# COMMAND ----------


