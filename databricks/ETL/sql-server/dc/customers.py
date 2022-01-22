# Databricks notebook source
# Python standard library
import sys
import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse, quote_plus

# Python open source libraries
from datetime import datetime, timedelta
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType, StructType, \
    StructField, DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Python DutchChannels libraries
from dc_azure_toolkit import cosmos_db
from dc_azure_toolkit import sql
from data_sql.main import main as data_sql_main

# COMMAND ----------

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*8*2 

ENVIRONMENT = 'prd'

# COMMAND ----------

def to_datetime_(column, formats=("yyyy-MM-dd'T'HH:mm:ss.SSSS", "yyyy-MM-dd'T'HH:mm:ss:SSSS", "yyyy-MM-dd'T'HH:mm:ss")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_timestamp(column, f) for f in formats])

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

# Configuration below tweaked based on "Learning Spark, 2nd edition, page 180".
spark = SparkSession \
    .builder \
    .appName("Customized spark session for daily_report notebook.") \
    .config("spark.driver.memory", "2g") \
    .config("spark.shuffle.file.buffer", "1m") \
    .config("spark.file.transferTo", "true") \
    .config("spark.shuffle.unsafe.file.output.buffer", "1m") \
    .config("spark.io.compression.lz4.blockSize", "512k") \
    .config("spark.shuffle.service.index.cache.size", "100m") \
    .config("spark.shuffle.registration.timeout", "120000ms") \
    .config("spark.shuffle.registration.maxAttempts", "3") \
    .config("spark.memory.fraction", "0.7") \
    .config("spark.memory.storageFraction", "0.4") \
    .config("spark.sql.shuffle.partitions", NR_PARTITIONS) \
    .getOrCreate()

# COMMAND ----------

ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)

authentication_dict = ctx['SQLAuthenticator'].get_authentication_dict()

subscriptions_dfs_without_fakes = sql.select_table("subscriptions", authentication_dict).filter(col('customer_id')!='00000000-0000-0000-0000-000000000000') # Not making fake customers for fake subscriptions

# COMMAND ----------

auth_dict = CosmosAuthenticator('uds-prd-db-account').get_authentication_dict()
query = """
SELECT 
c.id as customer_id, 
c.channel, 
c.registration_country, 
c.create_date, 
c.activation_date, 
c.last_login, 
(IS_DEFINED(c.email) AND LENGTH(c.email) < 50 ? c.email : '') AS email, 
(IS_DEFINED(c.facebook_handle) ? c.facebook_handle : '') AS facebook_id, 
(IS_DEFINED(c.first_name) AND LENGTH(c.first_name) < 50 ? c.first_name : '') AS first_name, 
(IS_DEFINED(c.last_name) AND LENGTH(c.last_name) < 50 ? c.last_name : '') AS last_name 
FROM c
"""
customer_dfs = cosmos_db.select(query, auth_dict, 'uds-db', 'axinom-user', 'dfs')
customer_dfs = customer_dfs.withColumn('customer_id', upper(col('customer_id')))
del auth_dict, query

# COMMAND ----------

customer_dfs = customer_dfs.join(subscriptions_dfs_without_fakes.select('customer_id', 'country', 'subscription_create_date', 'first_payment_date'), 'customer_id', 'left_outer')

# COMMAND ----------

customer_dfs = customer_dfs \
      .withColumn("create_date", to_datetime_(col('create_date'))) \
      .withColumn("activation_date", to_datetime_(col('activation_date'))) \
      .withColumn("last_login", to_datetime_(col('last_login'))) \
      .withColumn("first_payment_date", to_datetime_(col('first_payment_date')))

# COMMAND ----------

artificial_customers_dfs = subscriptions_dfs_without_fakes.join(customer_dfs, 'customer_id', 'left_anti')
artificial_customers_dfs = artificial_customers_dfs \
  .withColumn("registration_country", col('country')) \
  .withColumn("create_date", to_datetime_(col('subscription_create_date'))) \
  .withColumn("first_payment_date", col('first_payment_date')) \
  .withColumn("first_name", lit(None).cast(StringType())) \
  .withColumn("last_name", lit(None).cast(StringType())) \
  .withColumn("create_date", col('subscription_create_date')) \
  .withColumn("activation_date", lit(None).cast(TimestampType())) \
  .withColumn("last_login", lit(None).cast(TimestampType())) \
  .withColumn("facebook_id", lit(None).cast(StringType())) \
  .withColumn("email", lit(None).cast(StringType()))

# Window to take the first row per customer (by subscription_create_date)
window_first_sub_create_date = Window.partitionBy("customer_id").orderBy(col("subscription_create_date"))
artificial_customers_dfs = artificial_customers_dfs.withColumn("row",row_number().over(window_first_sub_create_date)) \
        .filter(col("row") == 1).drop("row")

# COMMAND ----------

# Window to take first payment date per customer
window_first_payment_date = Window.partitionBy("customer_id").orderBy(col("first_payment_date"))
first_payment_per_customer = customer_dfs.filter(col("first_payment_date").isNotNull()).withColumn("row",row_number().over(window_first_payment_date)) \
        .filter(col("row") == 1).drop("row").select('customer_id', 'first_payment_date')

# Window to take the last row (depended on subscription_create_date) per customer. This takes customer data (same for all rows) + last country (changes per row).
window_last_sub_create_date = Window.partitionBy("customer_id").orderBy(col("subscription_create_date").desc())
customer_dfs = customer_dfs.withColumn("row",row_number().over(window_last_sub_create_date)) \
        .filter(col("row") == 1).drop("row", 'first_payment_date')

customer_dfs = customer_dfs.join(first_payment_per_customer, 'customer_id', 'leftouter')
artificial_customers_dfs = artificial_customers_dfs.select(customer_dfs.columns)
final_customer_dfs = customer_dfs.union(artificial_customers_dfs)

# COMMAND ----------

final_customer_dfs = final_customer_dfs.select(
    'customer_id',
    'channel',
    'country',
    'first_name',
    'last_name',
    'registration_country',
    'create_date',
    'activation_date',
    'last_login',
    'facebook_id',
    'email',
    'first_payment_date')

# COMMAND ----------

ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)

customers_service = ctx['CustomersService']

insert_passed = customers_service.run_insert(final_customer_dfs)

if insert_passed == False:
    sys.exit("Error, customer data is not inserted!")

# COMMAND ----------


