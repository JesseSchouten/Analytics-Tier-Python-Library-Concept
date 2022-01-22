# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.functions import udf, array, lit, col

import sys
import os

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import hashlib
#dbutils.fs.rm("/FileStore/tables/subscription_plan_id.csv")

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

resource_name ='uelprdadx'
kustoDatabase = 'playground'

query = """
subscriptions
| project Date,CustomerId = ['Customer.Id'], SubscriptionId = ['Subscription.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionStartDate = ['Subscription.SubscriptionStart'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], CustomerRegistrationCountry = ['Customer.RegistrationCountry'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id'], SubscriptionPlanBillingFrequency = ['SubscriptionPlan.BillingFrequency'], AdyenPayments = ['Adyen.Payment.Payments'], AdyenDisputes = ['Adyen.Dispute.Disputes'], RecurringEnabled = ['Subscription.RecurringEnabled'], AdyenFirstPaymentReceived = ['Adyen.Payment.FirstPaymentReceived'], AdyenTotalAmountInSettlementCurrency = ['Adyen.Payment.TotalAmountInSettlementCurrency'], SubscriptionPrice = ['SubscriptionPlan.Price'], SubscriptionCurrency = ['SubscriptionPlan.Currency']
| where Date >= startofday(datetime("2021-1-25")) and Date < startofday(datetime("2021-1-25") + 1d)
"""

authentication_dict = get_data_explorer_authentication_dict(resource_name)

pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
kustoOptions = {"kustoCluster":authentication_dict['cluster'] 
               ,"kustoDatabase":kustoDatabase
              ,"kustoAadAppId":authentication_dict['client_id'] 
              ,"kustoAadAppSecret":authentication_dict['client_secret']
              , "kustoAadAuthorityID":authentication_dict['authority_id']}

dfs_adf  = pyKusto.read. \
          format("com.microsoft.kusto.spark.datasource"). \
          option("kustoCluster", kustoOptions["kustoCluster"]). \
          option("kustoDatabase", kustoOptions["kustoDatabase"]). \
          option("kustoQuery", query). \
          option("kustoAadAppId", kustoOptions["kustoAadAppId"]). \
          option("kustoAadAppSecret", kustoOptions["kustoAadAppSecret"]). \
          option("kustoAadAuthorityID", kustoOptions["kustoAadAuthorityID"]). \
          load()
df_adf = dfs_adf.toPandas()

# COMMAND ----------

query = """
SELECT * FROM default.facebook_handles
"""
dfs_facebook_handles = spark.sql(query)
del query
df_facebook_handles = dfs_facebook_handles.toPandas()


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


# COMMAND ----------

df_ADF_lastcreateddate['SubscriptionEndDate'].iloc[1]
df_ADF_lastcreateddate= pd.merge(df_ADF_lastcreateddate,df_facebook_handles,how='left',left_on = ['CustomerId'],right_on = ['Id'])

# COMMAND ----------

def is_churned(x):
  if x < datetime.now().date():
    return 1
  else: return 0
df_ADF_lastcreateddate['churned'] = df_ADF_lastcreateddate['SubscriptionEndDate'].apply(lambda x: is_churned(x))
df_ADF_lastcreateddate['Facebook_handle'] = df_ADF_lastcreateddate['Facebook_handle'].fillna('null')

# COMMAND ----------


df_ADF_fbh = df_ADF_lastcreateddate[df_ADF_lastcreateddate['Facebook_handle'] != 'null']
df_ADF_fbh = df_ADF_fbh[df_ADF_fbh['churned'] == 0]
df_ADF_fbh[['Platform','CustomerId']].groupby('Platform').count()

# COMMAND ----------

resource_name ='uelprdadx'
kustoDatabase = 'uel'

wl_userid_list = df_ADF_fbh[df_ADF_fbh['Platform'] == 'WL']['CustomerId'].unique().tolist()
nfn_userid_list = df_ADF_fbh[df_ADF_fbh['Platform'] == 'NFN']['CustomerId'].unique().tolist()

query = """
tracking
| where platform == 'Web'
| distinct userId
"""


pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
kustoOptions = {"kustoCluster":authentication_dict['cluster'] 
               ,"kustoDatabase":kustoDatabase
              ,"kustoAadAppId":authentication_dict['client_id'] 
              ,"kustoAadAppSecret":authentication_dict['client_secret']
              , "kustoAadAuthorityID":authentication_dict['authority_id']}


dfs_viewing  = pyKusto.read. \
          format("com.microsoft.kusto.spark.datasource"). \
          option("kustoCluster", kustoOptions["kustoCluster"]). \
          option("kustoDatabase", kustoOptions["kustoDatabase"]). \
          option("kustoQuery", query). \
          option("kustoAadAppId", kustoOptions["kustoAadAppId"]). \
          option("kustoAadAppSecret", kustoOptions["kustoAadAppSecret"]). \
          option("kustoAadAuthorityID", kustoOptions["kustoAadAuthorityID"]). \
          load()
df_viewing = dfs_viewing.toPandas()
df_viewing_wl = df_viewing[df_viewing['userId'].isin(wl_userid_list)]
df_viewing_nfn = df_viewing[df_viewing['userId'].isin(nfn_userid_list)]

# COMMAND ----------

wl_web = df_viewing_wl['userId'].unique().tolist()
nfn_web = df_viewing_nfn['userId'].unique().tolist()
print("{} of 2987 WL user showed activity on web.".format(len(wl_web)))
print("{} of 5474 NFN user showed activity on web.".format(len(nfn_web)))

# COMMAND ----------

resource_name ='uelprdadx'
kustoDatabase = 'uel'

wl_userid_list = df_ADF_fbh[df_ADF_fbh['Platform'] == 'WL']['CustomerId'].unique().tolist()
nfn_userid_list = df_ADF_fbh[df_ADF_fbh['Platform'] == 'NFN']['CustomerId'].unique().tolist()

query = """
tracking
| where platform in ('Android', 'Ios', 'IosOld') 
| distinct userId
"""


pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
kustoOptions = {"kustoCluster":authentication_dict['cluster'] 
               ,"kustoDatabase":kustoDatabase
              ,"kustoAadAppId":authentication_dict['client_id'] 
              ,"kustoAadAppSecret":authentication_dict['client_secret']
              , "kustoAadAuthorityID":authentication_dict['authority_id']}


dfs_viewing  = pyKusto.read. \
          format("com.microsoft.kusto.spark.datasource"). \
          option("kustoCluster", kustoOptions["kustoCluster"]). \
          option("kustoDatabase", kustoOptions["kustoDatabase"]). \
          option("kustoQuery", query). \
          option("kustoAadAppId", kustoOptions["kustoAadAppId"]). \
          option("kustoAadAppSecret", kustoOptions["kustoAadAppSecret"]). \
          option("kustoAadAuthorityID", kustoOptions["kustoAadAuthorityID"]). \
          load()
df_viewing = dfs_viewing.toPandas()
df_viewing_wl = df_viewing[df_viewing['userId'].isin(wl_userid_list)]
df_viewing_nfn = df_viewing[df_viewing['userId'].isin(nfn_userid_list)]

# COMMAND ----------

wl_app = df_viewing_wl['userId'].unique().tolist()
nfn_app = df_viewing_nfn['userId'].unique().tolist()
print("{} of 2987 WL user showed activity on Ios or android.".format(len(wl_app)))
print("{} of 5474 NFN user showed activity on Ios or android.".format(len(nfn_app)))

# COMMAND ----------


