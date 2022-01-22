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
from urllib.parse import urlparse, quote_plus

# Python open source libraries
import pandas as pd
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType, StructType, \
    StructField
from pyspark.sql.functions import udf, array, lit, col

# Python DutchChannels libraries
from dc_azure_toolkit import cosmos_db
from data_sql.main import main as data_sql_main

# COMMAND ----------

COUNTRY_LIST = ['NL', 'BE', 'GB', 'IE', 'UK', 'NZ', 'AU', 'SE', 'NO']

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

auth_dict = CosmosAuthenticator('uds-prd-db-account').get_authentication_dict()
query = """
SELECT 
c.id,
c.channel,
c.pause_max_days,
c.system,
c.free_trial,
c.billing_cycle_type,
c.description,
c.price,
c.asset_type,
c.only_available_with_promotion,
c.billing_frequency,
c.recurring,
c.catch_up_hours,
c.pause_min_days,
c.number_of_supported_devices,
c.currency,
c.business_type,
c.is_downloadable,
c.only_available_for_logged_in_users,
c.title,
c.start,
c["end"],
c.subscription_plan_type
FROM c where c.channel != 'hor'
"""
df = cosmos_db.select(query, auth_dict, 'uds-db', 'axinom-subscription-plan', 'df')
del auth_dict, query


# COMMAND ----------

def extract_country(subscription_plan_id, channel, title, country_list):
    """
  Extract country of subscription_plan_id from a raw (json) dictionary.
  """
    if channel == 'vg':
        return 'NL'

    for country in country_list:
        if country.lower() in title.lower():
            return country.upper()
        elif country.lower() in subscription_plan_id.lower():
            return country.upper()

        if subscription_plan_id in ['0-11-lov-month-plan', '0-11-svodsubscriptionplan', '0-11-test-plan']:
            return 'NL'

    return None


cols = [
    'id',
    'channel',
    'country',
    'pause_max_days',
    'system',
    'free_trial',
    'billing_cycle_type',
    'description',
    'price',
    'asset_type',
    'only_available_with_promotion',
    'billing_frequency',
    'recurring',
    'catch_up_hours',
    'pause_min_days',
    'number_of_supported_devices',
    'currency',
    'business_type',
    'is_downloadable',
    'only_available_for_logged_in_users',
    'title',
    'start',
    'end',
    'subscription_plan_type'
]

df['country'] = df[['id', 'channel', 'title']].apply(lambda x: extract_country(x[0], x[1], x[2], COUNTRY_LIST), axis=1)
final_df = df[cols]
final_df['free_trial'].fillna(0, inplace=True)
payload_items = final_df.values.tolist()

# COMMAND ----------

ctx = data_sql_main(environment='prd', dbutils=dbutils)

subscriptionplan_service = ctx['SubscriptionPlanService']

insert_passed = subscriptionplan_service.run_insert(payload_items, batch_size=100)

if insert_passed == False:
    sys.exit("Error, subscription plan data is not inserted!")
