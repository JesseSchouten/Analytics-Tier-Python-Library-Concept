# Databricks notebook source
# Python standard library
import sys

# Python open source libraries
from datetime import datetime
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql.functions import *

# Python DutchChannels libraries
from dc_azure_toolkit import sql
from data_sql.main import main as data_sql_main

# COMMAND ----------

# Hardcoded string which represents the start of DutchChannels
START = '2017-12-01'

# Hardcoded to and including which month to iterate
now = datetime.now()
END = '%s-%s-01' % (now.year, now.month)

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*8*2 

# Config for which DB we target, either dev or prd
ENVIRONMENT = 'prd'

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

customers_dfs = sql.select_table("customers", authentication_dict, spark=spark) \
  .repartition(NR_PARTITIONS, "create_date", "customer_id") \
  .withColumnRenamed('first_payment_date', 'customer_first_payment_date') \
  .cache()

subscriptions_dfs = sql.select_table("subscriptions", authentication_dict, spark=spark) \
  .repartition(NR_PARTITIONS, "subscription_create_date", "customer_id", "subscription_id") \
  .filter(col('state') != 'pending') \
  .cache()

payments_dfs = sql.select_table("payments", authentication_dict, spark=spark) \
  .repartition(NR_PARTITIONS, "creation_date", "merchant_reference") \
  .cache()

# COMMAND ----------

def create_month_state_between_dates(start, end):
  customers = customers_dfs \
    .filter(col('create_date').isNotNull() & (col('create_date') < end))

  customer_cols = customers \
    .withColumn('cohort', date_format(col('create_date'), 'yyyy-MM')) \
    .withColumn('age', \
                (date_format(end, 'yyyy').cast(IntegerType()) - date_format(col('create_date'), 'yyyy').cast(IntegerType())) * 12 \
                + (date_format(end, 'MM').cast(IntegerType()) - date_format(col('create_date'), 'MM').cast(IntegerType()) - 1)) \
    .withColumn('new_customer', when(col('create_date') > start, lit(True)).otherwise(lit(False))) \
    .select('customer_id', 'cohort', 'age', 'new_customer')

  subscriptions = subscriptions_dfs \
    .filter(col('subscription_create_date') < end) \
    .join(customers, 'customer_id')

  subscription_cols = subscriptions \
    .withColumn('new_subscription', when(col('subscription_create_date') > start, lit(1)).otherwise(lit(0))) \
    .groupBy('customer_id').agg(sum('new_subscription').alias('new_subscription_sum'), \
                                count('subscription_id').alias('subscription_count'), \
                                max('subscription_end').alias('subscription_end'), \
                                min('customer_first_payment_date').alias('first_payment_date')) \
    .withColumn('is_active', when(col('subscription_end') > start, lit(True)).otherwise(lit(False))) \
    .withColumn('has_new_subscription', when(col('new_subscription_sum') > 0, lit(True)).otherwise(lit(False))) \
    .withColumn('is_trialist', when((col('new_subscription_sum') > 0) \
                                    & (col('first_payment_date').isNull() | (col('first_payment_date') > start)), \
                                    lit(True)).otherwise(lit(False))) \
    .withColumn('is_reconnect', when((col('new_subscription_sum') > 0) \
                                     & (col('is_trialist') == False), \
                                     lit(True)).otherwise(lit(False))) \
    .select('customer_id', 'is_active', 'has_new_subscription', 'is_trialist', 'is_reconnect')

  payments = payments_dfs \
    .filter((col('merchant_reference').isNotNull()) & (col('creation_date') < end)) \
    .join(subscriptions.alias('subscriptions'), col('merchant_reference') == col('subscriptions.subscription_id'))

   # Note that we are making the assumption here that all gross payments are in the same currency
  payment_cols = payments  \
    .filter(col('creation_date') > start) \
    .groupBy('customer_id').agg(sum('gross_debit').alias('gross_debit'), \
                                sum('gross_credit').alias('gross_credit'), \
                                sum('gross_received').alias('gross_received'), \
                                max('net_currency').alias('currency'), \
                                sum('net_debit').alias('net_debit'), \
                                sum('net_credit').alias('net_credit'), \
                                sum('net_received').alias('net_received'), \
                                sum('commission').alias('commission'), \
                                sum('markup').alias('markup'), \
                                sum('scheme_fees').alias('scheme_fees'), \
                                sum('interchange').alias('interchange'), \
                                sum('total_fees').alias('total_fees'), \
                                sum(col('has_chargeback').cast('long')).alias('has_chargeback'), \
                                sum(col('has_reversed_chargeback').cast('long')).alias('has_reversed_chargeback'), \
                                sum(col('has_refund').cast('long')).alias('has_refund')) \
    .withColumn('has_chargeback', when(col('has_chargeback') > 0, lit(True)).otherwise(lit(False))) \
    .withColumn('has_reversed_chargeback', when(col('has_reversed_chargeback') > 0, lit(True)).otherwise(lit(False))) \
    .withColumn('has_refund', when(col('has_refund') > 0, lit(True)).otherwise(lit(False))) \
    .select('customer_id', 'currency', 'gross_debit', 'gross_credit', 'gross_received', \
            'net_debit', 'net_credit', 'net_received', \
            'commission', 'markup', 'scheme_fees', 'interchange', 'total_fees', \
            'has_chargeback', 'has_reversed_chargeback', 'has_refund')

  payment_total_cols = payments  \
    .groupBy('customer_id').agg(sum('gross_debit').alias('gross_debit_total'), \
                                sum('gross_credit').alias('gross_credit_total'), \
                                sum('gross_received').alias('gross_received_total'), \
                                sum('net_debit').alias('net_debit_total'), \
                                sum('net_credit').alias('net_credit_total'), \
                                sum('net_received').alias('net_received_total'), \
                                sum('commission').alias('commission_total'), \
                                sum('markup').alias('markup_total'), \
                                sum('scheme_fees').alias('scheme_fees_total'), \
                                sum('interchange').alias('interchange_total'), \
                                sum('total_fees').alias('total_fees_total')) \
    .withColumn('is_paying', when(col('net_received_total') > 1, lit(True)).otherwise(lit(False))) \
    .select('customer_id', 'is_paying', 'gross_debit_total', 'gross_credit_total', 'gross_received_total', \
            'net_debit_total', 'net_credit_total', 'net_received_total', \
            'commission_total', 'markup_total', 'scheme_fees_total', 'interchange_total', 'total_fees_total')

  # Note that this is an inner join, we are only interested in customer records with any subscription
  customer_month_state = customers.select('customer_id', 'channel').withColumn('month', lit(start)) \
    .join(customer_cols, ['customer_id'], 'leftouter') \
    .join(subscription_cols, ['customer_id']) \
    .join(payment_cols, ['customer_id'], 'leftouter') \
    .join(payment_total_cols, ['customer_id'], 'leftouter') \
    .fillna(0).fillna("").fillna(False)

  return customer_month_state

# COMMAND ----------

dateRange = pd.date_range(start = START, end = END, freq ='MS')
print("Iterating over {} periods.".format(dateRange.size))

outtimer = time.time()

insert_passed_list = []
for start in dateRange:
  timer = time.time()
  
  end = start + pd.DateOffset(months=1)
  print("Start {} to {}.".format(start, end))
  
  month_state = create_month_state_between_dates(to_timestamp(lit(start)), to_timestamp(lit(end)))
  print("Duration %.2f s (found records: %s). Starting to insert." % (time.time() - timer, month_state.count()))
  
  insert_passed = ctx['CustomerMonthStatesService'].run_insert(month_state)
  insert_passed_list.append(insert_passed)
  print("Done! Iteration duration %.2f s. Aggregated duration %.2f s." % (time.time() - timer, time.time() - outtimer))
  
customers_dfs.unpersist()
subscriptions_dfs.unpersist()
payments_dfs.unpersist()

# COMMAND ----------

if False in insert_passed_list:
  sys.exit("Error, Customer Month State data contained error!")

# COMMAND ----------


