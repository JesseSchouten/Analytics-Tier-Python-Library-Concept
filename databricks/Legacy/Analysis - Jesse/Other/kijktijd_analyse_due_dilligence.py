# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib

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

# COMMAND ----------

#prd:
cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" #
client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" #
authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,client_id,client_secret, authority_id)

client = KustoClient(kcsb)


# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/subscription_plan_id.csv")

# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/pushnotifications_test2_userbase.csv")
db ='playground'

query_ADF = """
subscriptions
| project Date=Date, SubscriptionId = ['Subscription.Id'], CustomerId = ['Customer.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id']
| where Date >= startofday(datetime('2021-1-1')) and Date < startofday(datetime('2021-1-1') + 1d)
| project SubscriptionId, CustomerId, SubscriptionCreatedDate, SubscriptionEndDate, Channel, SubscriptionPlanId;
"""
response = client.execute(db, query_ADF)
df_adf = dataframe_from_result_table(response.primary_results[0])


db = "uel"
query_watchtime = """
set truncationmaxrecords=1000000;
tracking
| where timestamp >= datetime('2020-12-1') and timestamp < datetime('2021-1-1')
| summarize playtime=round(toreal(sum(videoPlayTime)) / (60)) by bin(timestamp, 1d), userId;
"""

query_watchtime = """
set truncationmaxrecords=1000000;
tracking
| where videoPlayTime >=0 and timestamp >= datetime('2020-12-1') and timestamp < datetime('2021-1-4')
| project timestamp, userId, videoPlayTime
| summarize playtime = round(toreal(sum(videoPlayTime)) / 60)  by timestamp = bin(AdjustTimezone(timestamp,0), 1d), userId 
| join (tracking
        | where videoPlayTime >= 0 and action in ("View") and timestamp >= datetime('2020-12-1') and timestamp < datetime('2021-1-4')
        | project timestamp, userId,videoId, videoType, platform, action
        | summarize video_views = count() by timestamp = bin(AdjustTimezone(timestamp,0), 1d), userId, action
        | project timestamp, userId,video_views
        ) on timestamp, userId
| project timestamp, userId, playtime, video_views
;
"""

response = client.execute(db, query_watchtime)
df_watchtime = dataframe_from_result_table(response.primary_results[0])

query = """
SELECT d1.subscription_id, d1.channel_country, d1.channel, d1.country, d1.contract_duration, d2.market
FROM static_tables.subscription_plan_id as d1 
left join static_tables.country_market as d2 
on
d1.country = d2.country;
"""
dfs_subscription_plan_id = spark.sql(query)
df_subscription_plan_id = dfs_subscription_plan_id.toPandas()

# COMMAND ----------

df_adf_temp = df_adf[['CustomerId','SubscriptionCreatedDate']].groupby('CustomerId').max().reset_index()

df_adf_temp

# COMMAND ----------

df_ADF_lastcreateddate = pd.merge(df_adf_temp,df_adf,how='left',left_on = ['CustomerId','SubscriptionCreatedDate'],right_on = ['CustomerId','SubscriptionCreatedDate'])

#df_adf = pd.merge(df_adf,df_subscription_plan_id[['subscription_id', 'channel_country']],how='left',left_on = ['SubscriptionPlanId'],right_on = ['subscription_id'])
  
df_ADF_lastcreateddate=pd.merge(df_ADF_lastcreateddate,df_subscription_plan_id[['subscription_id','channel_country']],how='left',left_on = ['SubscriptionPlanId'],right_on = ['subscription_id'])

del df_adf_temp

# COMMAND ----------

import math
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
final_df = pd.DataFrame()

dates = list(df_watchtime['timestamp'].unique())
dates.sort()
#date = dates[31]
for date in dates:
  #df_ADF_lastcreateddate['SubscriptionCreatedDate'] = df_ADF_lastcreateddate['SubscriptionCreatedDate'].apply(lambda x: x.date())
  #CHECK WITH ARON
  adf_at_date = df_ADF_lastcreateddate[df_ADF_lastcreateddate['SubscriptionEndDate'] >= date + timedelta(1)]
  adf_at_date = adf_at_date[adf_at_date['SubscriptionCreatedDate'] < date + timedelta(1)]

  df_watchtime_at_date = df_watchtime[df_watchtime['timestamp'] == date]
  day_df = pd.merge(df_watchtime_at_date,adf_at_date,how='right',left_on = ['userId'],right_on=['CustomerId'])

  day_df['timestamp'] = date
  #test['playtime'] = 0
  #day_df = day_df[day_df['channel_country'] == 'NFN NL']

  cols = ['timestamp',
   'SubscriptionCreatedDate',
   'SubscriptionId',
   'SubscriptionEndDate',
   'Channel',
   'SubscriptionPlanId',
   'subscription_id',
   'channel_country',
    'CustomerId'
    ,'playtime'
     ,'video_views']

  group_cols = ['timestamp',
   'SubscriptionCreatedDate',
   'SubscriptionId',
   'SubscriptionEndDate',
   'Channel',
   'SubscriptionPlanId',
   'subscription_id',
   'channel_country',
    'CustomerId'
  ]

  print('Date: {}'.format(date))
  #print("Number of unique users in ADF export today: {}".format(len(adf_at_date['SubscriptionId'].unique())))
  day_df = day_df[cols].groupby(group_cols).sum().reset_index()
  print("Number of unique users after merging viewing data with ADF export: {}".format(len(day_df['SubscriptionId'].unique().tolist())))

  day_df['Duration'] = day_df['timestamp'] - day_df['SubscriptionCreatedDate'] + timedelta(1)
  day_df['Duration'] = day_df['Duration'].apply(lambda x: x.total_seconds() / (60*60*24))
  day_df['Duration_month'] = day_df['Duration'].apply(lambda x: round_duration(x))
  day_df['Duration_quarter'] = day_df['Duration_month'].apply(lambda x: to_quarters(x))
  day_df

  final_df = pd.concat((final_df,day_df))
final_df

# COMMAND ----------

def mark_as_viewed(x):
  if x >= 1:
    return 1
  else:
    return 0

#final_df['viewed'] = final_df['playtime'].apply(lambda x: mark_as_viewed(x))
final_df['viewed'] = final_df['video_views'].apply(lambda x: mark_as_viewed(x))
final_df['year_month'] = final_df['timestamp'].apply(lambda x: str(x.year) + "-" +str(x.month))
#final_df['year'] = final_df['timestamp'].apply(lambda x: str(x.year))
#final_df['month'] = final_df['timestamp'].apply(lambda x: str(x.month))
#final_df['day'] = final_df['timestamp'].apply(lambda x: str(x.day))
final_df['week'] = final_df['timestamp'].apply(lambda x: str(x.week))
final_df

# COMMAND ----------

#remove trial users
platforms = ['NFN NL', 'NFN SE', 'NFN NO', 'NFN GB','NFN IE', 'NFN NZ', 'NFN AU']
select_cols = ['timestamp','SubscriptionCreatedDate','SubscriptionId','SubscriptionEndDate','SubscriptionPlanId','channel_country','playtime','CustomerId','Duration','Duration_month','Duration_quarter','viewed','year_month','week']
final_df = final_df[select_cols]
filter_trials = True
if filter_trials:
  final_df = final_df[final_df['Duration_month'] != 0]
final_df = final_df[final_df['viewed'] == 1]
final_df = final_df[final_df['channel_country'].isin(platforms)]
#dfs = spark.createDataFrame(final_df)
#dfs.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/df/viewing_data_no_trials.csv")
#export the data

# COMMAND ----------

def substitute_gb_ie(x):
  if x == 'NFN GB':
    return "NFN GB+IE"
  elif x == 'NFN IE':
    return "NFN GB+IE"
  return x


calc_df = final_df[final_df['channel_country'].isin(channels)]
calc_df['channel_country'] = calc_df['channel_country'].apply(lambda x: substitute_gb_ie(x))
calc_df = calc_df[calc_df['week'].isin(['53','52','51','50'])]

calc_df = calc_df[['timestamp','week','playtime','SubscriptionId','channel_country']].groupby(['week','SubscriptionId','timestamp','channel_country']).agg({'playtime':'sum'}).reset_index()
calc_df = calc_df[['timestamp','week','playtime','SubscriptionId','channel_country']].groupby(['week','channel_country']).agg({'playtime':'sum','SubscriptionId':'nunique','timestamp':'nunique'}).reset_index()
calc_df['avg_playtime/user'] = (calc_df['playtime']/60) / (calc_df['SubscriptionId'])
print(calc_df[calc_df['week'] == '50'][['SubscriptionId','timestamp','channel_country','playtime','avg_playtime/user']])
calc_df = calc_df[['week','avg_playtime/user','channel_country']].groupby(['channel_country']).agg({'avg_playtime/user':'mean'})
#use the average number of customers per day, to account for new customers and leaving customers

calc_df

# COMMAND ----------

calc_df = final_df[final_df['channel_country'].isin(channels)]
calc_df['channel_country'] = calc_df['channel_country'].apply(lambda x: substitute_gb_ie(x))
calc_df = calc_df[calc_df['week'].isin(['53','52','51','50'])]

calc_df = calc_df[['timestamp','SubscriptionId','viewed','channel_country']].groupby(['channel_country']).agg({'SubscriptionId':'nunique','timestamp':'nunique','viewed':'sum'}).reset_index()

#use the average number of customers per day, to account for new customers and leaving customers
calc_df['avg_views/user'] = calc_df['viewed'] / calc_df['SubscriptionId']
calc_df

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

channels = ['NFN NL','NFN SE', 'NFN NO', 'NFN NZ', 'NFN AU','NFN IE','NFN GB']
#channels = ['TT NL']
#channels = ['HOR NL']
#channels = ['WL NL', 'WL SE', 'WL NO']
calc_df = final_df[final_df['channel_country'].isin(channels)]

calc_df = calc_df[['timestamp','week','playtime','SubscriptionId','viewed','channel_country']].groupby(['week','channel_country']).agg({'playtime':'sum','SubscriptionId':'last','timestamp':'nunique','viewed':'sum'}).reset_index()
calc_df = calc_df[calc_df['week'].isin(['52','51','50','49'])]
#use the average number of customers per day, to account for new customers and leaving customers
calc_df['avg_playtime/user'] = calc_df['playtime'] / (calc_df['SubscriptionId'])
calc_df['avg_views/user'] = calc_df['viewed'] / calc_df['SubscriptionId']
#calc_df['avg_playtime/user'] = calc_df['avg_playtime/user'] * calc_df['timestamp']
#calc_df = calc_df[calc_df['Duration_quarter'] < 12]
total_df = calc_df

display(calc_df)

# COMMAND ----------

['NFN NL', 'TT NL', 'HOR NL', 'WL NL', 'NFN SE', 'NFN NO', 'NFN GB','NFN IE', 'NFN NZ', 'NFN AU', 'WL SE', 'WL NO']
channels = ['NFN NL','NFN SE', 'NFN NO', 'NFN NZ', 'NFN AU','NFN IE','NFN GB']
#channels = ['TT NL']
#channels = ['HOR NL']
#channels = ['WL NL', 'WL SE', 'WL NO']
calc_df = final_df[final_df['channel_country'].isin(channels)]
calc_df = calc_df[['timestamp','year_month','playtime','SubscriptionId']].groupby(['year_month']).agg({'playtime':'sum','SubscriptionId':'nunique','timestamp':'nunique'}).reset_index()
calc_df['avg_playtime/user'] = calc_df['playtime'] / calc_df['SubscriptionId']
#calc_df['avg_playtime/user'] = calc_df['avg_playtime/user'] * calc_df['timestamp']
#calc_df = calc_df[calc_df['Duration_quarter'] < 12]
display(calc_df)

# COMMAND ----------



# COMMAND ----------

channels = ['NFN NL','NFN SE', 'NFN NO', 'NFN NZ', 'NFN AU','NFN IE','NFN GB']
#channels = ['TT NL']
#channels = ['HOR NL']
#channels = ['WL NL', 'WL SE', 'WL NO']
calc_df = final_df[final_df['channel_country'].isin(channels)]
calc_df = calc_df[['timestamp','Duration_quarter','playtime','SubscriptionId','viewed']].groupby(['Duration_quarter']).agg({'playtime':'sum','SubscriptionId':'nunique','timestamp':'nunique','viewed':'sum'}).reset_index()
#calc_df['SubscriptionId'] = calc_df['SubscriptionId'] / calc_df['timestamp']
calc_df['avg_playtime/user'] = calc_df['playtime'] / calc_df['SubscriptionId']
calc_df['avg_views/user'] = calc_df['viewed'] / calc_df['SubscriptionId']
#calc_df['avg_playtime/user'] = calc_df['avg_playtime/user'] * calc_df['timestamp']
#calc_df = calc_df[calc_df['Duration_quarter'] < 12]
total_df = calc_df

display(calc_df)

# COMMAND ----------

['NFN NL', 'TT NL', 'HOR NL', 'WL NL', 'NFN SE', 'NFN NO', 'NFN GB','NFN IE', 'NFN NZ', 'NFN AU', 'WL SE', 'WL NO']
channels = ['NFN NL','NFN SE', 'NFN NO', 'NFN NZ', 'NFN AU','NFN IE','NFN GB']
#channels = ['TT NL']
#channels = ['HOR NL']
#channels = ['WL NL', 'WL SE', 'WL NO']
calc_df = final_df[final_df['channel_country'].isin(channels)]
calc_df = calc_df[['timestamp','Duration_quarter','playtime','SubscriptionId']].groupby(['Duration_quarter']).agg({'playtime':'sum','SubscriptionId':'nunique','timestamp':'nunique'}).reset_index()
calc_df['avg_playtime/user'] = calc_df['playtime'] / calc_df['SubscriptionId']
#calc_df['avg_playtime/user'] = calc_df['avg_playtime/user'] * calc_df['timestamp']
#calc_df = calc_df[calc_df['Duration_quarter'] < 12]
display(calc_df)

# COMMAND ----------

final_df = final_df[final_df['Duration_month'] != 0]

temp_df = final_df[final_df['playtime'] > 0]
temp_df =temp_df[temp_df['Duration_month'] <=13]
temp_df = temp_df[['SubscriptionId','Duration_month','playtime']].groupby(['SubscriptionId','Duration_month']).count().reset_index()
print(temp_df[['Duration_month','playtime']].groupby('Duration_month').mean())

display(temp_df)

# COMMAND ----------

import seaborn

fig, ax1 = plt.subplots(figsize=(10,6))

color = 'tab:blue'
ax1.set_title('Customers by subscription length', fontsize=16)
ax1.set_xlabel('Duration (quarters)', fontsize=10)

sns.barplot(x=calc_df.index, y='SubscriptionId', ax=ax1, data = calc_df, color=color)
ax1.tick_params(axis='y')
ax1.set_ylabel('Unique customers (#)',fontsize=10)

#specify we want to share the same x-axis
ax2 = ax1.twinx()
color = 'tab:red'
#bar plot creation
ax2.set_ylabel('Avg playtime/user', fontsize=16)
sns.lineplot(x=calc_df.index, y='avg_playtime/user', ax=ax2, data=calc_df, sort=False, color=color)
ax2.tick_params(axis='y', color=color)
ax2.set_ylabel('Avg playtime per unique user (minutes)',fontsize=10)
ax2.set_xlabel('Subscription length (in quarters)',fontsize=10)
#line plot creation

#show plot



# COMMAND ----------

display(calc_df)

# COMMAND ----------


