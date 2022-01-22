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
    StructField, DateType, BooleanType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Python DutchChannels libraries
from dc_azure_toolkit import cosmos_db
from dc_azure_toolkit import sql
from data_sql.main import main as data_sql_main

# COMMAND ----------

ENVIRONMENT = 'prd'

# COMMAND ----------

def to_datetime_(column, formats=("yyyy-MM-dd'T'HH:mm:ss.SSSS", "yyyy-MM-dd'T'HH:mm:ss:SSSS", "yyyy-MM-dd'T'HH:mm:ss")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_timestamp(column, f) for f in formats])

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*8*2 

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

subscriptions_plan_dfs = sql.select_table("subscription_plan", authentication_dict, spark=spark)
payments_dfs = sql.select_table("payments", authentication_dict, spark=spark)

# COMMAND ----------

auth_dict = CosmosAuthenticator('uds-prd-db-account').get_authentication_dict()
query = """
SELECT
    c.id as subscription_id,
    c.customer_id,
    c.channel,
    c.subscription_plan_id,
    c.created_date as subscription_create_date,
    c.subscription_start,
    c.subscription_end,
    c.subscription_paused_date ?? null as subscription_paused_date,
    c.subscription_resumable_days ?? null as subscription_resumable_days,
    c.state,
    c.recurring_enabled,
    c.payment_provider,
    c.free_trial ?? 0 as free_trial,
    c.notes,
    c.is_pausable,
    c.is_resumable,
    c.new_plan_id ?? c.newPlanId ?? null as new_plan_id
FROM c
"""
subscriptions_dfs = cosmos_db.select(query, auth_dict, 'uds-db', 'axinom-subscription', 'dfs')
subscriptions_dfs = subscriptions_dfs.withColumn('subscription_id', upper(col('subscription_id')))
del auth_dict, query

# COMMAND ----------

def add_first_payment_column(subscription_dfs):
  payments_info_dfs = sql.select_table("payments_info", authentication_dict)
  payments_info_dfs = payments_info_dfs.na.fill(0, ['commission', 'markup', 'scheme_fees','interchange', 'gross_debit', 'gross_credit', 'net_debit', 'net_credit'])
  payments_info_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['Chargeback', 'Settled', 'Refunded', 'ChargebackReversed', 'RefundedReversed']))
  payments_info_cb_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['Chargeback']))
  payments_info_cb_rev_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['ChargebackReversed']))
  payments_info_refunded_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['Refunded']))
  payments_info_refunded_rev_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['RefundedReversed']))

  payments_dfs = payments_info_dfs.select(['psp_reference', 'merchant_reference']).dropDuplicates()

  # Add net_currency
  temp_dfs = payments_info_dfs.select(['psp_reference', 'net_currency']).dropDuplicates()
  payments_dfs = payments_dfs.join(temp_dfs, 
                                   ['psp_reference'], \
                                   "left") \

  # Add net_debit and net_credit
  temp_dfs = payments_info_dfs.select(['psp_reference', 'net_debit', 'net_credit']) \
            .groupby(['psp_reference']) \
            .sum() \
            .withColumnRenamed('sum(net_debit)', 'net_debit') \
            .withColumnRenamed('sum(net_credit)', 'net_credit') \
            .withColumn("net_received", expr("net_credit - net_debit")) \

  payments_dfs = payments_dfs.join(temp_dfs, 
                                   ['psp_reference'], \
                                   "left") \

  # Add net_received
  payments_dfs = payments_dfs.withColumn('net_received', (payments_dfs['net_credit'] - payments_dfs['net_debit']))   

  # Add has_chargeback
  payments_info_cb_dfs = payments_info_cb_dfs.withColumn("has_chargeback", lit(1))
  payments_dfs = payments_dfs.join(payments_info_cb_dfs.select(['psp_reference','has_chargeback']), \
                                  ['psp_reference'], \
                                  "left") \
                              .fillna(0)

  # Add has_reversed_chargeback
  payments_info_cb_rev_dfs = payments_info_cb_rev_dfs.withColumn("has_reversed_chargeback", lit(1))
  payments_dfs = payments_dfs.join(payments_info_cb_rev_dfs.select(['psp_reference','has_reversed_chargeback']), \
                                   ['psp_reference'], \
                                  "left") \
                              .fillna(0)

  # Add has_refund
  payments_info_refunded_dfs = payments_info_refunded_dfs.withColumn("has_refund", lit(1))
  payments_dfs = payments_dfs.join(payments_info_refunded_dfs.select(['psp_reference','has_refund']), \
                                   ['psp_reference'], \
                                  "left") \
                              .fillna(0)

  # Add has_reverse_refund
  payments_info_refunded_rev_dfs = payments_info_refunded_rev_dfs.withColumn("has_reversed_refund", lit(1))
  payments_dfs = payments_dfs.join(payments_info_refunded_rev_dfs.select(['psp_reference','has_reversed_refund']), \
                                   ['psp_reference'], \
                                  "left") \
                              .fillna(0)

  payments_dfs = payments_dfs.withColumn("is_valid_payment" \
                                         , when((col("has_chargeback") - col('has_reversed_chargeback') == 0) \
                                                & (col("has_refund") - col('has_reversed_refund') == 0) \
                                                & (col("net_credit") > 1) \
                                               , 1).otherwise(0)) \
                            .filter(col("is_valid_payment") == 1) 

  temp_df = payments_info_dfs.select(['psp_reference', 'creation_date']) \
                             .groupBy(['psp_reference']) \
                             .agg(min("creation_date").alias("first_payment_date"))

  payments_dfs = payments_dfs.join(temp_df, ['psp_reference'], 'leftouter') \
                .select(['merchant_reference', 'first_payment_date']) \
                .groupBy(['merchant_reference']) \
                .agg(min("first_payment_date").alias("first_payment_date")) \
                .withColumnRenamed('merchant_reference', 'subscription_id') \
                .withColumn("subscription_id", upper("subscription_id"))

  subscription_dfs = subscription_dfs.join(payments_dfs, ['subscription_id'], "left")

  return subscription_dfs

# COMMAND ----------

subscriptions_dfs = subscriptions_dfs.join(subscriptions_plan_dfs.select('id', 'country').withColumnRenamed('id', 'subscription_plan_id'), 'subscription_plan_id', 'leftouter')
subscriptions_dfs = add_first_payment_column(subscriptions_dfs)

# Subscription plans with no country filling it with NL
subscriptions_dfs = subscriptions_dfs.withColumn('country', 
    when(col('subscription_plan_id')=='0-11-dc-test-plan', lit('NL')) \
   .when(col('subscription_plan_id')=='0-11-horrify-premium', lit('NL')) \
   .when(col('subscription_plan_id')=='0-11-hor-halfyear-plan', lit('NL')) \
   .when(col('subscription_plan_id')=='0-11-hor-year-plan', lit('NL')) \
   .otherwise(subscriptions_dfs.country))

# Converting strings to datetime
subscriptions_dfs = subscriptions_dfs \
      .withColumn("subscription_create_date", to_datetime_(col('subscription_create_date'))) \
      .withColumn("subscription_start", to_datetime_(col('subscription_start'))) \
      .withColumn("subscription_paused_date", to_datetime_(col('subscription_paused_date'))) \
      .withColumn("first_payment_date", to_datetime_(col('first_payment_date')))

# .withColumn("first_payment_date", from_unixtime(unix_timestamp(col('first_payment_date'),"yyyy-MM-dd'T'HH:mm:ss.SSS")))

#result.withColumn("subscription_resumable_days",result.account_id.cast(StringType()))
#result.withColumn("new_plan_id",result.account_id.cast(StringType()))

subscriptions_dfs = subscriptions_dfs \
      .withColumn("subscription_resumable_days", \
       when(col('subscription_resumable_days').isNull(), lit(0)) \
       .otherwise(col('subscription_resumable_days'))) \
      .withColumn("new_plan_id", \
       when(col('new_plan_id').isNull(), lit(None).cast(StringType())) \
       .otherwise(col('new_plan_id')))

# COMMAND ----------

payments_dfs = payments_dfs \
                .withColumnRenamed("merchant_reference", 'subscription_id') \
                .filter(col('subscription_id').isNotNull())

artificial_subscriptions_dfs = payments_dfs.join(subscriptions_dfs, 'subscription_id', 'left_anti')
artificial_subscriptions_dfs = artificial_subscriptions_dfs \
  .withColumn("customer_id", lit("00000000-0000-0000-0000-000000000000")) \
  .withColumn("subscription_plan_id", lit("NULL"))\
  .withColumn("subscription_create_date", col('creation_date')) \
  .withColumn("subscription_start", col('creation_date')) \
  .withColumn("subscription_end", col('creation_date')) \
  .withColumn("subscription_paused_date", lit(None).cast(TimestampType())) \
  .withColumn("subscription_resumable_days", lit(None).cast(IntegerType())) \
  .withColumn("state", lit(None).cast(StringType())) \
  .withColumn("recurring_enabled", lit(False)) \
  .withColumn("payment_provider", lit(None).cast(StringType())) \
  .withColumn("free_trial", lit(None).cast(IntegerType())) \
  .withColumn("notes", lit(None).cast(StringType())) \
  .withColumn("is_pausable", lit(None).cast(BooleanType())) \
  .withColumn("is_resumable", lit(None).cast(BooleanType())) \
  .withColumn("new_plan_id", lit(None).cast(StringType())) \
  .withColumn("first_payment_date", col('creation_date'))

# COMMAND ----------

# Take first row by subscription creaed date
window_first_sub_create_date = Window.partitionBy("subscription_id").orderBy(col("subscription_create_date"))

artificial_subscriptions_dfs = artificial_subscriptions_dfs.withColumn("row",row_number().over(window_first_sub_create_date)) \
        .filter(col("row") == 1).drop("row")

artificial_subscriptions_dfs = artificial_subscriptions_dfs.select(subscriptions_dfs.columns)

# COMMAND ----------

final_subscriptions_dfs = subscriptions_dfs.union(artificial_subscriptions_dfs)
final_subscriptions_dfs = final_subscriptions_dfs.select(
    'subscription_id', 
    'customer_id',
    'channel',
    'country',
    'subscription_plan_id',
    'subscription_create_date',
    'subscription_start',
    'subscription_end',
    'subscription_paused_date',
    'subscription_resumable_days',
    'state',
    'recurring_enabled',
    'payment_provider',
    'free_trial',
    'notes',
    'is_pausable',
    'is_resumable',
    'new_plan_id',
    'first_payment_date')
#     #.fillna(0).fillna("").fillna(False)

# COMMAND ----------

ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)

subscription_service = ctx['SubscriptionsService']

insert_passed = subscription_service.run_insert(final_subscriptions_dfs, batch_size=100)

if insert_passed == False:
    sys.exit("Error, subscription data is not inserted!")
