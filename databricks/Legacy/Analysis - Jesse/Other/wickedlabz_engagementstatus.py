# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib

# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/withlove_movies.csv")

# COMMAND ----------

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

import pandas as pd
import numpy as np
import itertools
from datetime import timedelta,datetime

# COMMAND ----------

#prd:
cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" #
client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" #
authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,client_id,client_secret, authority_id)

client = KustoClient(kcsb)

# COMMAND ----------

db ='playground'

query_ADF = """
subscriptions
| project Date=Date, SubscriptionId = ['Subscription.Id'], CustomerId = ['Customer.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id']
| where Date >= startofday(datetime('2021-1-12')) and Date < startofday(datetime('2021-1-12') + 1d)
| project SubscriptionId, CustomerId, SubscriptionCreatedDate, SubscriptionEndDate, Channel, SubscriptionPlanId;
"""
response = client.execute(db, query_ADF)
df_adf = dataframe_from_result_table(response.primary_results[0])



# COMMAND ----------

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
df_ADF_lastcreateddate = pd.merge(df_adf_temp,df_adf,how='left',left_on = ['CustomerId','SubscriptionCreatedDate'],right_on = ['CustomerId','SubscriptionCreatedDate'])
df_ADF_lastcreateddate= pd.merge(df_ADF_lastcreateddate,df_subscription_plan_id[['subscription_id','channel_country']],how='left',left_on = ['SubscriptionPlanId'],right_on = ['subscription_id'])

del df_adf_temp

# COMMAND ----------

df_ADF_lastcreateddate = df_ADF_lastcreateddate[df_ADF_lastcreateddate['channel_country'] == 'NFN NL']

# COMMAND ----------

def exclude_last_characters(x, exclude_nr = 15):
    return x[:-exclude_nr]

wickedlabs_query = """
SELECT *
FROM default.NFN_Wickedlabz
;
"""
wickedlabs_dfs = spark.sql(wickedlabs_query)
wickedlabs_df = wickedlabs_dfs.toPandas()
wickedlabs_df['subscriptionId'] = wickedlabs_df['subscriptionId'].apply(lambda x: exclude_last_characters(x))
wickedlabs_df

# COMMAND ----------

merge_df = pd.merge(df_ADF_lastcreateddate,wickedlabs_df, how='left',left_on = 'SubscriptionId',right_on = 'subscriptionId')
es_list = ['inactive']
merge_df = merge_df[merge_df['engagementstatus'].isin(es_list)]
#merge_df['engagementstatus'].unique()
inactive_list = merge_df['CustomerId'].unique().tolist()
len(inactive_list)

# COMMAND ----------


inactive_tuple = tuple(inactive_list)

db = "uel"
query_watchtime = """
tracking
| where userId in {} and timestamp >= datetime('2021-1-12') - 21d and timestamp < datetime('2021-1-12')
| summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by bin(timestamp + 1h, 1d), userId;
""".format(inactive_tuple)

response = client.execute(db, query_watchtime)
df_inactive = dataframe_from_result_table(response.primary_results[0])


# COMMAND ----------

df_inactive = df_inactive[df_inactive['playtime'] > 5]
print(len(df_inactive['userId'].unique().tolist()))
print(df_inactive['userId'].unique().tolist())


# COMMAND ----------

db = "uel"
query_watchtime = """
tracking
| where userId in {} and timestamp >= datetime('2021-1-12') - 21d and timestamp < datetime('2021-1-12')
| summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by bin(timestamp + 1h, 1d), userId,platform;
""".format(tuple(df_inactive['userId'].unique().tolist()))

response = client.execute(db, query_watchtime)
df_inactive = dataframe_from_result_table(response.primary_results[0])
print(len(df_inactive['userId'].unique().tolist()))
df_inactive['userId'].unique().tolist()

# COMMAND ----------

df_inactive[['platform','playtime']].groupby('platform').count().rename(columns={'playtime':'nr_instances'})


# COMMAND ----------


