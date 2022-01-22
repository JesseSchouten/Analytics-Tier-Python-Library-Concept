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

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

import pandas as pd
import numpy as np
from datetime import datetime,timedelta

# COMMAND ----------

"""
This script can be used to draw random samples from userid's from a specific platform and country, who have the app. 
This is developed for the purpose of sending pushnotifications.

Steps:
- Get all userid's for platforms
- Combine with ADF export and subscription_id_csv to get the country where the userid is from.
- Filter the unwanted users based on some global variables indicated in this cell.
  - Potential filters:
    - Engagement statuses dormant or neverviewed.
    - cases where the channel_country is not found (unknown userid's)
    - countries not indicated in the global variable.
- Draw random samples, divide in 2 or 3 groups.
- Write to a table, perhaps export the userids by hand in list type format.
"""

PLATFORM = 'WL'
NUMBER_OF_SAMPLE_GROUPS = 1
PLATFORM_COUNTRY = 'NL' #DEFAULT, CAN BE CHANGED LATER IN CELL!

WICKETLABS_DATASET_NAME_DBFS = '{}_wicketlabs'.format(PLATFORM).lower()

#Checks to make sure the provided parameters are supported.
if NUMBER_OF_SAMPLE_GROUPS not in [1, 2, 3]:
  sys.exit("We only support 2 or 3 sample groups right now!")
if PLATFORM not in ['NFN', 'WL']:
  sys.exit(f"Platform {PLATFORM} is currently not supported!")


# COMMAND ----------

def get_wl_userids():
    url = "https://us-central1-withlove-ee0a2.cloudfunctions.net/push/ids"
    payload={}
    headers = {
      'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZjc0YzUwMDEtNjI2NC00OTc4LTk0NzYtNGEwZmNhNjU1OGI0Iiwic3lzdGVtIjoiSW50ZXJuYWwiLCJ1c2VyX2VtYWlsIjoic2VydmljZXNAZHV0Y2hjaGFubmVscy5ubCIsImFjdGl2YXRpb25fZGF0ZSI6IjIwMjAtMDEtMDdUMTA6MDI6NDYuNzczIiwiY3JlYXRlZF9kYXRlIjoiMjAyMC0wNi0xMVQxMjo0NDoyNy41MDUxODA1WiIsInJlZ2lzdHJhdGlvbl9jb3VudHJ5IjoiTkwifQ.gCmvTU-ooZ0g4ZnF7bvsdHJMmCpKqt07QamsIlFLs_c',
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.text

def get_nfn_userids():
    """
    Retrieves all NFN users who have the app. 
    """
    url = "https://us-central1-newfaithandroid.cloudfunctions.net/push/ids"
    payload={}
    headers = {
      'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.text

def run(platform):
    supported_platforms = ['NFN','WL'] 
    if platform not in supported_platforms:
        print('ERROR: something went wrong, platform not allowed (row 43)')
        sys.exit()
    
    if platform.upper() == 'NFN':
        user_ids = get_nfn_userids()
    elif platform.upper() == 'WL':
        user_ids = get_wl_userids()
    else:
        print('ERROR: something went wrong, platform not allowed (row 52)')
        sys.exit()
    
    return user_ids
        
user_ids_with_app_list = ast.literal_eval(run(PLATFORM))

# COMMAND ----------

df_customer_id = pd.DataFrame(user_ids_with_app_list,columns=['customer_id'])

#prd:
cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" #
client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" #
authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,client_id,client_secret, authority_id)

client = KustoClient(kcsb)

db ='playground'

query_ADF = """
subscriptions
| project Date, subscription_id = ['Subscription.Id'], customer_id = ['Customer.Id'], subscription_created_date = ['Subscription.CreatedDate'], subscription_end_date = ['Subscription.SubscriptionEnd'], subscription_plan_id = ['SubscriptionPlan.Id'], customer_registration_country = ['Customer.RegistrationCountry']
| where Date >= startofday(datetime('2021-7-30')) and Date < startofday(datetime('2021-7-30')+ 1d)
| project subscription_id, customer_id, subscription_created_date, subscription_end_date, subscription_plan_id, customer_registration_country;
"""
response = client.execute(db, query_ADF)
df_ADF = dataframe_from_result_table(response.primary_results[0])

# query = """
# SELECT d1.subscription_id, d1.channel_country, d1.channel, d1.country, d1.contract_duration, d2.market
# FROM static_tables.subscription_plan_id as d1 
# left join static_tables.country_market as d2 
# on
# d1.country = d2.country;
# """
# dfs_subscription_plan_id = spark.sql(query)
# df_subscription_plan_id = dfs_subscription_plan_id.toPandas().rename(columns = {'subscription_id':'subscription_plan_id'})

# del response, dfs_subscription_plan_id
# gc.collect()

# COMMAND ----------

# Python DutchChannels libraries
from dc_azure_toolkit import cosmos_db

def get_authentication_dict_cosmosdb(resource_name):
  """
  Get the dictionary with required authentication credentials for connection to a given cosmos database instance.
  
  :param resource_name: name of the cosmosDB resource.
  :type resource_name: str
  
  """
  if resource_name not in ['uds-prd-db-account']:
    return None
  if resource_name == 'uds-prd-db-account':
    result = {'account_url' : 'https://uds-prd-db-account.documents.azure.com:443/'
             ,'account_key': dbutils.secrets.get(scope = "data-prd-keyvault", key = "uds-prd-db-account-accountkey")
             }
  return result

auth_dict = get_authentication_dict_cosmosdb('uds-prd-db-account')
query = 'SELECT * FROM c'
df = cosmos_db.select(query, auth_dict, 'uds-db', 'axinom-subscription-plan')

def extract_country(subscription_plan_id, channel, title, country_list):
  """
  Extract country of subscription_plan_id from a raw (json) dictionary.
  """
  
  if channel == 'vg' or channel == 'hor':
    return 'NL'
  
  for country in country_list:
    if country.lower() in title.lower():
      return country.upper()
    elif country.lower() in subscription_plan_id.lower():
      return country.upper() 
    
    if subscription_plan_id == '0-11-lov-month-plan':
      return 'NL'
    elif subscription_plan_id == '0-11-svodsubscriptionplan':
      return 'NL'
  return None

def transform_platform(x):
  if x == 'LOV':
    return 'WL'
  else:
    return x

country_list = ['NL', 'BE', 'GB', 'IE', 'UK', 'NZ', 'AU', 'SE', 'NO']

df['country'] = df[['id','channel','title']].apply(lambda x: extract_country(x[0],x[1],x[2],country_list), axis =1)
df_subscription_plan_id = df[['id', 'channel','country', 'billing_frequency']].rename(columns = {'id':'subscription_plan_id'})
df_subscription_plan_id['channel'] = df_subscription_plan_id['channel'].apply(lambda x :x.upper())
df_subscription_plan_id['channel'] = df_subscription_plan_id['channel'].apply(lambda x: transform_platform(x))
df_subscription_plan_id['channel_country'] = df_subscription_plan_id['channel'] + "-"+ df_subscription_plan_id['country']
df_subscription_plan_id[0:1]

# COMMAND ----------

def transform_data_types(df_customer_id, df_subscription_plan_id, df_ADF):
    df_ADF['subscription_created_date'] = pd.to_datetime(df_ADF['subscription_created_date'], format="%m/%d/%Y")
    df_ADF['subscription_end_date'] = pd.to_datetime(df_ADF['subscription_end_date'], format="%m/%d/%Y")
    
    return df_customer_id, df_subscription_plan_id, df_ADF

def filter_out_duplicates(df_ADF):
    """
    Filters out customer_id's who have had multiple contracts, takes the last known subscription_plan_id based on the created_date. 
    """
    print("{} unique customer ids in original df".format(len(df_ADF['customer_id'].unique())))
    print("{} total ids".format(len(df_ADF)))
    group_df = df_ADF[['customer_id','subscription_id','subscription_plan_id','customer_registration_country','subscription_created_date']].sort_values('subscription_created_date').groupby(['customer_id']).tail(1)
    group_df = group_df.reset_index()
    group_df
    print("{} customer ids with a contract".format(len(group_df['customer_id'].unique())))
    return group_df

df_customer_id, df_subscription_plan_id, df_ADF = transform_data_types(df_customer_id, df_subscription_plan_id, df_ADF)
df_ADF = filter_out_duplicates(df_ADF)

result_df = pd.merge(df_customer_id,df_ADF,how='left',left_on=['customer_id'],right_on=['customer_id']) 
print('Total number of customer ids: {}'.format(len(result_df)))
print("Rows with no subscription_plan_id: {}".format(len(result_df[result_df['subscription_plan_id'].isnull()])))

df = pd.merge(result_df, df_subscription_plan_id,how='left',left_on=['subscription_plan_id'],right_on=['subscription_plan_id'])[['customer_id','subscription_id','subscription_plan_id','channel_country','customer_registration_country']]

end_df=df

del df_customer_id, df_subscription_plan_id, df_ADF, result_df
gc.collect()

# COMMAND ----------

# def exclude_last_characters(x, exclude_nr = 15):
#     return x[:-exclude_nr]

# def remove_underscores(x):
#     return x.replace('-','')

# def get_engagement_status_df(df):
#     ES_df = df[['customerId','subscriptionId','engagementstatus']]
#     ES_df['subscriptionId'] = ES_df['subscriptionId'].apply(lambda x:exclude_last_characters(x))
#     return ES_df

# query = """
# SELECT * FROM default.{}
# """.format(WICKETLABS_DATASET_NAME_DBFS)
# wicketlabs_df = spark.sql(query)
# wicketlabs_df = wicketlabs_df.toPandas()


# ES_df = get_engagement_status_df(wicketlabs_df)

# def merge_dfs_for_engagement_status(df,select_date = None):
#     if type(select_date) == type(None):
#         ES_df = get_engagement_status_df()
#     else:
#         ES_df = get_engagement_status_df(select_date)
        
# end_df = pd.merge(df, ES_df,how = 'left',left_on = 'subscription_id',right_on='subscriptionId')  
# end_df = end_df[['customer_id', 'subscription_id', 'subscription_plan_id',
#        'channel_country', 'engagementstatus']]



# COMMAND ----------

# def adjust_engagement_status(x):
#     if x == 'new':
#         return 'engaged'
#     elif x == 'reEngaged':
#         return 'engaged'
#     else:
#         return x

# def check_engagement_statusses(df):
#     unique_engagement_status_list = df['engagementstatus'].unique().tolist()
#     for es in unique_engagement_status_list:
#         if es not in ['engaged','idle','recent','inactive']:
#             print("ERROR, {} is unexpected engagement status".format(es))

# end_df = end_df[~end_df['engagementstatus'].isin(['dormant','neverViewed'])]
# end_df['engagementstatus'] = end_df['engagementstatus'].apply(lambda x:adjust_engagement_status(x))
# check_engagement_statusses(end_df)
# end_df = end_df[~end_df['engagementstatus'].isna()]
# end_df

# COMMAND ----------

overwrite = True
if overwrite:
  PLATFORM_COUNTRY = 'SE'

if PLATFORM_COUNTRY not in ['AU','NZ','NL','SE','NO','GB','BE','IE']:
  sys.exit(f"Platform Country {PLATFORM_COUNTRY} is currently not supported!")

end_df_country = end_df[end_df['channel_country'] == PLATFORM + '-' +PLATFORM_COUNTRY]

# COMMAND ----------

user_list = end_df_country['customer_id'].unique().tolist()

# COMMAND ----------

#Sample from group, create NUMBER_OF_SAMPLE_GROUPS groups

import argparse 
import ast
import json
import sys

import random
import pandas as pd

#random.seed(5)

def sample_without_replacement(user_list,sample_size):
    return random.sample(user_list,sample_size)

def create_experimental_groups(user_list, nr_groups = 3):
    if nr_groups == 1:
      return user_list, [], []
    if nr_groups == 2:
      nr_users = len(set(user_list))
      group_1 = sample_without_replacement(user_list,round(len(nr_users)/2))
      control_group = set(user_list) ^ set(group_1)
      return group_1, control_group,[]
    if nr_groups == 3:
      nr_users = len(set(user_list))
      group_1 = sample_without_replacement(user_list,round(2*nr_users/3))
      control_group = set(user_list) ^ set(group_1)
      group_2 = sample_without_replacement(group_1,round(len(group_1)/2))
      group_1 = set(group_1) ^ set(group_2)
      group_2 = set(group_2)
      return list(group_1),list(group_2),list(control_group)


group_1,group_2,control_group = create_experimental_groups(user_list, NUMBER_OF_SAMPLE_GROUPS)   
print("number of users after filters: {}".format(len(user_list)))
print("size of control group: {}".format(len(control_group)))
print("size of experimental group 1: {}".format(len(group_1)))
print("size of experimental group 2: {}".format(len(group_2)))

service_account = ['f780a3b6-a484-4c4b-9948-5413ca628bef']

# COMMAND ----------

print("You are exporting the control group of: {}".format(PLATFORM + ' ' +PLATFORM_COUNTRY))
print("You can export this file, and copy paste to an empty json file from excel (or directly from content below)")
dft = pd.DataFrame(columns = ['user_id'])
dft.loc[1]=[control_group]
display(dft)
#display(dft)

# COMMAND ----------

print("You are exporting experimental_group_1 of: {}".format(PLATFORM + ' ' +PLATFORM_COUNTRY))
dft = pd.DataFrame(columns = ['user_id'])
dft.loc[1]=[group_1]
display(dft)

# COMMAND ----------

print("You are exporting experimental_group_2 of: {}".format(PLATFORM + ' ' +PLATFORM_COUNTRY))
dft = pd.DataFrame(columns = ['user_id'])
dft.loc[1]=[group_2]
display(dft)
