# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib

# COMMAND ----------

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.functions import udf, array, lit, col

import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import requests

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

subscription_plan_id_tuple_nfnnl = tuple(df_subscription_plan_id[df_subscription_plan_id['channel_country'] == 'NFN NL']['subscription_id'].unique().tolist())
subscription_plan_id_tuple_nfnnl

# COMMAND ----------

query = """
set truncationmaxsize=1048576000;
subscriptions
| project Date,CustomerId = ['Customer.Id'], SubscriptionId = ['Subscription.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionStartDate = ['Subscription.SubscriptionStart'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], CustomerRegistrationCountry = ['Customer.RegistrationCountry'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id'], SubscriptionPlanBillingFrequency = ['SubscriptionPlan.BillingFrequency'], AdyenPayments = ['Adyen.Payment.Payments'], AdyenDisputes = ['Adyen.Dispute.Disputes'], RecurringEnabled = ['Subscription.RecurringEnabled'], AdyenFirstPaymentReceived = ['Adyen.Payment.FirstPaymentReceived'], AdyenTotalAmountInSettlementCurrency = ['Adyen.Payment.TotalAmountInSettlementCurrency'], SubscriptionPrice = ['SubscriptionPlan.Price'], SubscriptionCurrency = ['SubscriptionPlan.Currency'], SubscriptionState = ['Subscription.State']
| where Date >= startofday(now()+ 0d) and Date < startofday(now() + {}d)
"""

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
subscriptions
| where Date >= startofday(now()+ {}d) and Date < startofday(now() + 1d +{}d) and ['Subscription.State'] == 'paused' and ['SubscriptionPlan.Id'] in {}
| project Date, SubscriptionId =  ['Subscription.Id'], pause_date = ['Subscription.SubscriptionEnd'] + 1d
"""

def get_file_n_days_ago(query, n=0):
  global subscription_plan_id_tuple_nfnnl
  df_adf = run_data_explorer_query(query.format(n,n,subscription_plan_id_tuple_nfnnl), resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'df')
  return df_adf


def get_pauses_past_n_days(query, n=1):
  daily_pauses = {}
  
  #get nr of pauses for all today, and n prior days.
  for i in range(0,n+1):
    date = datetime.now().date() + timedelta(days = -i - 1)
    df_adf = get_file_n_days_ago(query, -i)
    df_adf['pause_date'] = df_adf['pause_date'].apply(lambda x: x.date())
    df_adf_paused_today = df_adf[df_adf['pause_date'] == date]
    daily_pauses[date] = len(df_adf_paused_today)
  return daily_pauses

print(get_pauses_past_n_days(query, 14))

# COMMAND ----------

query = """
subscriptions
| where Date >= startofday(now()+ {}d) and Date < startofday(now() + 1d +{}d) and ['Subscription.RecurringEnabled'] == 'false' and ['SubscriptionPlan.Id'] in {}
| project Date, SubscriptionId =  ['Subscription.Id']
"""

def get_file_n_days_ago(query, n=0):
  global subscription_plan_id_tuple_nfnnl
  df_adf = run_data_explorer_query(query.format(n,n,subscription_plan_id_tuple_nfnnl), resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'df')
  return df_adf

def get_switched_users(query, n=1):
  daily_switched_recurring_enabled_true = {}
  for i in range(0, n+1):
    date = datetime.now().date() + timedelta(days = -i - 1)
    if i == 0:
      df_adf_today = get_file_n_days_ago(query, -i)
    else: 
      df_adf_today = df_adf_yesterday
    df_adf_yesterday = get_file_n_days_ago(query, -i - 1)
    df_recurring_enabled_false_today = len(df_adf_today)
    df_recurring_enabled_false_yesterday = len(df_adf_yesterday)
    delta = df_recurring_enabled_false_today-df_recurring_enabled_false_yesterday
    daily_switched_recurring_enabled_true[date + timedelta()] = delta
    
  return daily_switched_recurring_enabled_true
get_switched_users(query, n=30)

# COMMAND ----------


