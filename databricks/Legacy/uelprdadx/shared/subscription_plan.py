# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType, StructType, StructField
from pyspark.sql.functions import udf, array, lit, col

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz

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
Country
| project country_id = CountryId 
"""
country_id_list = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'shared', df_format = 'df')['country_id'].unique().tolist()

query = """
Channel
| project channel_abbreviation = ChannelAbbreviation 
"""
channel_id_list = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'shared', df_format = 'df')['channel_abbreviation'].unique().tolist()

query = """
BillingFrequency
| project billing_frequency_id = BillingFrequencyId, type = Type 
"""
billing_frequency_df = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'shared', df_format = 'df')
billing_frequency_dict = pd.Series(billing_frequency_df.type.values,index=billing_frequency_df.billing_frequency_id).to_dict()

query = """
SubscriptionPlan
| project subscription_plan_id = SubscriptionPlanId
"""
subscription_plan_id_list = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'shared', df_format = 'df')['subscription_plan_id'].unique().tolist()

del billing_frequency_df

# COMMAND ----------

def get_request_url(channel, country):
  """
  Outputs a request to which a GET request can be send for a platform. Takes the channel and corresponding country as parameters.
  
  :param channel: dutchchannels channel, for example nfn, hor, wl and vg
  :param country: country of the specific channel
  :type channel: str
  :type country: str
  """
  return "https://dc-{}-ms-subscription.axprod.net/v1/subscriptionplan?country={}".format(channel, country)

def extract_country(x, channel, country_list):
  """
  Extract country of subscription_plan_id from a raw (json) dictionary.
  """
  
  if channel == 'vg' or channel == 'hor':
    return 'NL'
  
  for country in country_list:
    if country.lower() in x['title'].lower():
      return country.upper()
    elif country.lower() in x['id'].lower():
      return country.upper() 
    if x['id'] == '0-11-lov-month-plan':
      return 'NL'
  return None

def extract_contract_duration(x, billing_frequency_dict):
  """
  Extract contract duration of subscription_plan_id from a raw (json) dictionary.
  """
  try:
    return billing_frequency_dict[x['billing_frequency']]
  except KeyError:
    return None
  except:
    return None

def transform_channel(x):
  if x == 'vg':
    subscription_channel = 'tt'.upper()
  elif x == 'lov':
    subscription_channel = 'wl'.upper()
  else:
    subscription_channel = x.upper()
  return subscription_channel
  
df = pd.DataFrame(columns = ['date', 'subscription_plan_id', 'channel','country','contract_duration'])  

for channel in channel_id_list:
  for country in country_id_list:
    payload={}
    url = get_request_url(channel.lower(),country.lower())
    response = requests.request("GET", url, data=payload).json()
    for row in response:      
      if row['id'] not in df['subscription_plan_id'].unique().tolist() and row['id'] not in subscription_plan_id_list:
        subscription_channel = transform_channel(channel)
        
        if channel == 'vg' or channel == 'hor':
          subscription_country = 'NL'
        else:
          subscription_country = extract_country(row, channel, country_id_list)
        
        contract_duration = extract_contract_duration(row, billing_frequency_dict)
        if type(contract_duration) != type(None) and type(subscription_country) != type(None):
          try:
            row = {'date': row['start']
                 , 'subscription_plan_id': row['id']
                 , 'channel': subscription_channel
                 , 'country': subscription_country
                 ,'contract_duration': contract_duration}
          except KeyError:
            known_subscription_plan_id_exceptions = ['0-11-svodsubsc_1025361845']
            if row['id'] not in known_subscription_plan_id_exceptions :
              print("{} has no start key or id. Likely a test account.")
            continue        
        else:
          print("Something is wrong with subscription_plan_id: {}, contract_duration:{}, subscription_country:{}".format(row['id'],contract_duration, subscription_country))
          continue
        
        df = df.append(row, ignore_index=True)



# COMMAND ----------

if len(df[df['country'].isnull()]) >0:
  print("WARNING: country not found and skipped: {}".format(df[df['country'].isnull()]))
if len(df[df['channel'].isnull()]) > 0:
  print("WARNING: channel not found and skipped: {}".format(df[df['channel'].isnull()]))
if len(df[df['contract_duration'].isnull()]) > 0:
  print("WARNING: contract_duration not found and skipped: {}".format(df[df['contract_duration'].isnull()]))

df = df[~df['country'].isnull()]
df = df[~df['channel'].isnull()]
df = df[~df['contract_duration'].isnull()]

# COMMAND ----------

write = False

subscription_plan_id_list
loaded_subscription_id_list = df['contract_duration'].unique().tolist()
if len(df) > 0:
  write = True
  
  #This should never do anything, as we attempted to only load new subscription_plan_ids with the API call, but you never know.
  length_df_before = len(df)
  df = df[~df['subscription_plan_id'].isin(subscription_plan_id_list)]
  length_df_after = len(df)
  if length_df_before != length_df_after:
    print("WARNING: there is a bug in the code, we deleted some already present subscription_plan_ids just before writing the data to the destination table.")

# COMMAND ----------

cols = ['subscription_plan_id', 'channel','country', 'contract_duration']

schema = StructType([
  StructField('subscription_plan_id', StringType(), True),
  StructField('channel', StringType(), True),
  StructField('country', StringType(), True),
  StructField('contract_duration', StringType(), True)
  ])


dfs = spark.createDataFrame(df[cols], schema)
dfs = dfs.withColumn("Created", lit(str(datetime.now(pytz.timezone('Europe/Amsterdam')))))

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
write_to_data_explorer(dfs, 'SubscriptionPlan', resource_name, kustoDatabase, write)

# COMMAND ----------

nr_new_records = dfs.count()
print("Number of records loaded to destination: {}".format(nr_new_records))
dbutils.notebook.exit("success")
