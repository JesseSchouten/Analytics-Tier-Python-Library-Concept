# Databricks notebook source
# Python standard library
import sys
import re
import math

# Python open source libraries
from pyspark.sql.functions import col, udf, row_number, when, lit
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import pandas as pd
import time

# Python DutchChannels libraries
from data_sql.main import main as data_sql_main
from dc_azure_toolkit import blob

# COMMAND ----------

ENVIRONMENT = 'prd'

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*8*2

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
    .config("spark.sql.shuffle.partitions", NR_PARTITIONS) \
    .getOrCreate()

ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)

# COMMAND ----------

def concat_payments_info_channels_dfs(channel_list):
  """
  Function to retrieve a single spark dataframe for a list of channels. 
  This is assuming that each report is seperately stored in a different container, 
  which can be retrieve by the channel name.
  """
  storage_account_name = "dcinternal"
  authentication_dict = StorageAccountAuthenticator(storage_account_name).get_authentication_dict()

  file_path = 'adyen/reports/transformed/settlement_detail.csv'
  
  if (not isinstance(channel_list, list)) & (len(channel_list) > 0):
    sys.exit("Invalid list with channel, either wrong type or empty.")
    
  payments_info_dfs = None
  for channel in channel_list:
    container_name = "dc" + channel
    if isinstance(payments_info_dfs, type(None)):
      payments_info_dfs = blob.select_from_csv(storage_account_name, container_name, authentication_dict, file_path)
    else:
      payments_info_dfs = payments_info_dfs.union(blob.select_from_csv(account_name, container_name, authentication_dict, file_path))
  
  payments_info_dfs = payments_info_dfs
  return payments_info_dfs.repartition(NR_PARTITIONS)

def payments_info_drop_duplicates(payments_info_dfs):
    payments_info_dfs_final = payments_info_dfs.select(col("Company Account").alias('company_account'),
                                    col("Merchant Account").alias('merchant_account'),
                                    col("Psp Reference").alias('psp_reference'),
                                    col("Merchant Reference").alias('merchant_reference'),
                                    col("Payment Method").alias('payment_method'),
                                    col("Creation Date").alias('creation_date'),
                                    col("TimeZone").alias('time_zone'),
                                    col("Booking Date").alias('booking_date'),
                                    col("Booking Date TimeZone").alias('booking_date_time_zone'),
                                    col("Type9").alias('type'),
                                    col("Modification Reference").alias('modification_reference'),                               
                                    col("Gross Currency").alias('gross_currency'),
                                    col("Gross Debit (GC)").alias('gross_debit'),
                                    col("Gross Credit (GC)").alias('gross_credit'),                               
                                    col("Exchange Rate").alias('exchange_rate'),
                                    col("Net Currency").alias('net_currency'),
                                    col("Net Debit (NC)").alias('net_debit'),                               
                                    col("Net Credit (NC)").alias('net_credit'),
                                    col("Commission (NC)").alias('commission'),
                                    col("Markup (NC)").alias('markup'),                               
                                    col("Scheme Fees (NC)").alias('scheme_fees'),
                                    col("Interchange (NC)").alias('interchange'),
                                    col("Payment Method Variant").alias('payment_method_variant'),                               
                                    col("Batch Number").alias('batch_number')) \
                                    .withColumn('afterpay', when(col('merchant_reference').contains('hard-grace-expiry-queue'), lit('hard_grace')) \
                                                           .when(col('merchant_reference').contains('hard-grace-queue'), lit('soft_grace')) \
                                                           .otherwise(lit(None)))
    payments_info_dfs_final = payments_info_dfs_final.dropDuplicates()
    return payments_info_dfs_final  

def add_payment_received_date(payments_info_dfs):
  """
  Function to retrieve the payment received date per batch. This function assumes that
  the payment received date has to be the latest payment (of a batch) with type MerchantPayout, Balancetransfer or DepositCorrection.
  """
  # Window to take max creation date per batch number
  window_max_creation_date_per_batch = Window.partitionBy("batch_number").orderBy(col("creation_date").desc())
  
  max_creation_date_per_batch_dfs = payments_info_dfs.filter((col("type")=='MerchantPayout') | (col("type")=='Balancetransfer') | \
                                                             (col("type")=='DepositCorrection')).withColumn("row",row_number().over(window_max_creation_date_per_batch)) \
                                                             .filter(col("row") == 1).drop("row").select('batch_number', 'creation_date')
  
  max_creation_date_per_batch_dfs = max_creation_date_per_batch_dfs.withColumnRenamed('creation_date', 'payment_received_date')
  payments_info_dfs = payments_info_dfs.join(max_creation_date_per_batch_dfs, 'batch_number', 'leftouter')
  
  return payments_info_dfs
  
nfn_payments_info_dfs = concat_payments_info_channels_dfs(['nfn'])
nfn_payments_info_dfs = payments_info_drop_duplicates(nfn_payments_info_dfs)
nfn_payments_info_dfs = add_payment_received_date(nfn_payments_info_dfs)

lov_payments_info_dfs = concat_payments_info_channels_dfs(['lov'])
lov_payments_info_dfs = payments_info_drop_duplicates(lov_payments_info_dfs)
lov_payments_info_dfs = add_payment_received_date(lov_payments_info_dfs)
lov_payments_info_dfs = lov_payments_info_dfs.select(nfn_payments_info_dfs.columns) # Be certain same columns order

# Union
payments_info_dfs = nfn_payments_info_dfs.union(lov_payments_info_dfs)

# COMMAND ----------

def parse_uuid(uuid):
  """
  Parses UUID with inconsisten seperators (-) to UUID with consistent seperators.
  """
  try:
    regexp_search = re.search('(\w{8})\-?(\w{4})\-?(\w{4})\-?(\w{4})\-?(\w{12})', uuid, re.IGNORECASE)
    group_1 = regexp_search.group(1)
    group_2 = regexp_search.group(2)
    group_3 = regexp_search.group(3)
    group_4 = regexp_search.group(4)
    group_5 = regexp_search.group(5)
    result = f"{group_1}-{group_2}-{group_3}-{group_4}-{group_5}"
  except Exception as e:
    print(f'warning: uuid {uuid} does not convert to uuid - {e}')
    result = None
  
  return result

MerchantReferenceUdf = udf(lambda x: parse_uuid(x), StringType())
payments_info_dfs = payments_info_dfs.withColumn("merchant_reference", MerchantReferenceUdf("merchant_reference"))

# COMMAND ----------

payments_info_dfs = payments_info_dfs.select('company_account', 
                                              'merchant_account',
                                              'psp_reference', 
                                              'merchant_reference', 
                                              'payment_method', 
                                              'creation_date', 
                                              'time_zone', 
                                              'booking_date',
                                              'booking_date_time_zone', 
                                              'type',
                                              'modification_reference', 
                                              'gross_currency', 
                                              'gross_debit',
                                              'gross_credit', 
                                              'exchange_rate',
                                              'net_currency',
                                              'net_debit',
                                              'net_credit', 
                                              'commission', 
                                              'markup', 
                                              'scheme_fees',
                                              'interchange', 
                                              'payment_method_variant',
                                              'batch_number', 
                                              'payment_received_date',
                                              'afterpay')

# COMMAND ----------

payments_info_service = ctx['PaymentsInfoService']
insert_passed = payments_info_service.run_insert(payments_info_dfs)

# COMMAND ----------

if insert_passed == False:
  sys.exit("Error, Payments Info data is not inserted!")
