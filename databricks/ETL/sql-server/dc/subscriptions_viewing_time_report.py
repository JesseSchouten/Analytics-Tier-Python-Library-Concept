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
import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse, quote_plus, quote

# Python open source libraries
import pandas as pd
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType, StructType, StructField
from pyspark.sql.functions import udf, array, lit, col

# Python DutchChannels libraries
from dc_azure_toolkit import adx
from data_sql.main import main as data_sql_main

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

query = """
subscriptions
| where startofday(Date) == startofday(now()) and ['Subscription.State'] != 'Pending' and isnotempty(['SubscriptionPlan.Price'])
| project id = ['Subscription.Id']
, channel = Channel
, customer_id = ['Customer.Id']
, customer_registration_country = ['Customer.RegistrationCountry']
, subscription_created_date = ['Subscription.CreatedDate']
, subscription_subscription_start_date =['Subscription.SubscriptionStart']
, subscription_plan_id = ['SubscriptionPlan.Id']
, subscription_plan_billingcycle_type = ['SubscriptionPlan.BillingCycleType']
, subscription_plan_billingfrequency = ['SubscriptionPlan.BillingFrequency']
, subscription_plan_price = ['SubscriptionPlan.Price']
, subscription_plan_currency =['SubscriptionPlan.Currency']
"""
auth_dict = ADXAuthenticator('uelprdadx').get_authentication_dict()
df = adx.select(query, auth_dict, kustoDatabase = 'playground', df_format ='df')
payload_items = df.values.tolist()

del auth_dict, query

# COMMAND ----------

ctx = data_sql_main(environment='prd', dbutils=dbutils)

subscriptions_viewing_time_report_service = ctx['SubscriptionsViewingTimeReportService']

insert_passed = subscriptions_viewing_time_report_service.run_insert(payload_items, batch_size=5000)

if insert_passed == False:
  sys.exit("Error, subscriptions viewing time report data is not inserted!")

# COMMAND ----------


