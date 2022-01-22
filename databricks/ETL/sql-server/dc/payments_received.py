# Databricks notebook source
# Python standard library
import sys
import re
import math
import logging
from functools import reduce  

# Python open source libraries
from pyspark.sql.functions import col, udf
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StringType, TimestampType, DoubleType, LongType, IntegerType, StructType, StructField
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
import pandas as pd
import time

# Python DutchChannels libraries
from data_sql.main import main as data_sql_main
from dc_azure_toolkit import blob

# COMMAND ----------

ENVIRONMENT = 'prd'

CHANNEL_LIST = ['nfn','lov']

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*4*2

# Retrieve data from NUMBER_OF_DAYS number of days, taking the START_DATE as starting point, writing to the db every DAYS_BATCH_SIZE days
START_DATE = str(datetime.now().date() - timedelta(days=14))
END_DATE = str(datetime.now().date() - timedelta(days=1))
BATCH_SIZE = 30

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

# Configuration below tweaked based on "Learning Spark, 2nd edition, page 180".
spark = SparkSession \
    .builder \
    .appName("Customized spark session for payments_received notebook.") \
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

def create_base_dfs():
  """
  Create an empty spark dataframe for the payments_received table.
  """
  schema = StructType([ \
      StructField("Company Account",StringType(),True), \
      StructField("Merchant Account",StringType(),True), \
      StructField("Psp Reference",StringType(),True), \
      StructField("Merchant Reference",StringType(),True), \
      StructField("Payment Method",StringType(),True), \
      StructField("Creation Date",TimestampType(),True), \
      StructField("TimeZone",StringType(),True), \
      StructField("Currency",StringType(),True), \
      StructField("Amount",DoubleType(),True), \
      StructField("Type",StringType(),True), \
      StructField("Risk Scoring",IntegerType(),True), \
      StructField("Shopper Interaction",StringType(),True), \
      StructField("Shopper Country",StringType(),True), \
      StructField("Issuer Name",StringType(),True), \
      StructField("Issuer Id",StringType(),True), \
      StructField("Issuer Country",StringType(),True), \
      StructField("Shopper Email",StringType(),True), \
      StructField("Shopper Reference",StringType(),True), \
      StructField("3D Directory Response",StringType(),True), \
      StructField("3D Authentication Response",StringType(),True), \
      StructField("CVC2 Response",StringType(),True), \
      StructField("AVS Response",StringType(),True), \
      StructField("Acquirer Response",StringType(),True), \
      StructField("Raw Acquirer Response",StringType(),True), \
      StructField("Authorisation Code",StringType(),True), \
      StructField("Acquirer Reference",StringType(),True), \
      StructField("Payment Method Variant",StringType(),True), \
    ])
  dfs = spark.createDataFrame(data=[],schema=schema)
  return dfs

def parse_uuid(uuid):
  """
  Parses UUID with inconsistent seperators (-) to UUID with consistent seperators.
  
  :param uuid: string containing a UUID in the format 80b8f1b3dd2648f7b108a47255f5f09f.
  :type uuid: str
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
    result = ''
  
  return result

def unionAll(dfs_list):
  """
  Helper to union a list containing one or multiple spark Dataframes.
  
  :param dfs_list: list containing a number of equally formatted spark Dataframes.
  :type dfs_list: list(Spark.DataFrame)
  """
  return reduce(DataFrame.unionAll, tuple(dfs_list))  
  
def select_columns_payments_received_dfs(payments_received_dfs):  
  return payments_received_dfs \
        .select(col("Company Account").alias('company_account'), \
                col("Merchant Account").alias('merchant_account'), \
                col("Psp Reference").alias('psp_reference'),
                col("Merchant Reference").alias('merchant_reference'),
                col("Payment Method").alias('payment_method'),
                col("Creation Date").alias('creation_date'),
                col("TimeZone").alias('time_zone'),
                col("Currency").alias('currency'),
                col("Amount").alias('amount'),
                col("Type").alias('type'),
                col("Risk Scoring").alias('risk_scoring'),
                col("Shopper Interaction").alias('shopper_interaction'),
                col("Shopper Country").alias('shopper_country'),
                col("Issuer Name").alias('issuer_name'),
                col("Issuer Id").alias('issuer_id'),
                col("Issuer Country").alias('issuer_country'),
                col("Shopper Email").alias('shopper_email'),
                col("Shopper Reference").alias('shopper_reference'),
                col("3D Directory Response").alias('3d_directory_response'),
                col("3D Authentication Response").alias('3d_authentication_response'),
                col("CVC2 Response").alias('cvc2_response'),
                col("AVS Response").alias('avs_response'),
                col("Acquirer Response").alias('acquirer_response'),
                col("Raw Acquirer Response").alias('raw_acquirer_response'),
                col("Authorisation Code").alias('authorisation_code'),
                col("Acquirer Response").alias('acquirer_reference'),
                col("Payment Method Variant").alias('payment_method_variant'),
               )                         

def process_payments_received_dfs(payments_received_dfs):
    payments_received_dfs = select_columns_payments_received_dfs(payments_received_dfs)
    
    payments_received_dfs = payments_received_dfs.withColumn('creation_date', payments_received_dfs['creation_date'].cast(TimestampType()))
    payments_received_dfs = payments_received_dfs.dropDuplicates()
    
    MerchantReference = udf(lambda x: parse_uuid(x), StringType())
    payments_received_dfs = payments_received_dfs.withColumn("merchant_reference", MerchantReference("merchant_reference"))
    payments_received_dfs = payments_received_dfs.withColumn("psp_reference",payments_received_dfs.psp_reference.cast(StringType()))
    
    return payments_received_dfs

def get_dcinternal_storage_container_name(channel):
  return "dc" + channel

def get_payments_received_dfs(channel, date):
  parsed_date = date.replace('-', '_')
  storage_account_name = "dcinternal"
  authentication_dict = StorageAccountAuthenticator(storage_account_name).get_authentication_dict()
  file_path = f'adyen/reports/raw/received_payments_report_{parsed_date}.csv'
  container_name = get_dcinternal_storage_container_name(channel)
  
  try:
    payments_received_dfs = blob.select_from_csv(storage_account_name, container_name, authentication_dict, file_path)
  except AnalysisException:
    payments_received_dfs = create_base_dfs()
    
  return payments_received_dfs

def get_daily_payments_received_dfs(date):
  """
  Function to retrieve the payments_accounting reports (https://docs.adyen.com/pt/reporting/dcc-transactions-and-fees/received-payment-details-report).
  Currently stored csv's on the dcinternal storage account.
  
  :param date: Date in format yyyy-MM-dd, e.g. 2021-01-01
  :type date: str
  """
    
  channel_payments_received_dfs_list = []
  for channel in CHANNEL_LIST:  
    payments_received_dfs = get_payments_received_dfs(channel, date)
    payments_received_dfs = process_payments_received_dfs(payments_received_dfs)  
    channel_payments_received_dfs_list.append(payments_received_dfs)

  return unionAll(channel_payments_received_dfs_list).repartition(NR_PARTITIONS) 

# COMMAND ----------

date_list = pd.date_range(start = START_DATE, end = END_DATE, freq ='D')

insert_passed_list = []
daily_dfs_list = []
for date in date_list:
  timer = time.time()
  
  date_string = str(date.date())
  
  payments_received_dfs = get_daily_payments_received_dfs(date_string)
  daily_dfs_list.append(payments_received_dfs)
  
  if len(daily_dfs_list) == BATCH_SIZE:
    payments_received_dfs_final = unionAll(daily_dfs_list).repartition(NR_PARTITIONS)
    insert_passed = ctx['PaymentsReceivedService'].run_insert(payments_received_dfs_final)
    insert_passed_list.append(insert_passed)    
    daily_dfs_list.clear()
    print("Inserted untill date: {}".format(date))
    
  print("Duration %.2f s (found records: %s)" % (time.time() - timer, payments_received_dfs.count()))

if len(daily_dfs_list) > 0:
  payments_received_dfs_final = unionAll(daily_dfs_list).repartition(NR_PARTITIONS)
  insert_passed = ctx['PaymentsReceivedService'].run_insert(payments_received_dfs_final)
  insert_passed_list.append(insert_passed)
  
  print("Inserted untill date: {}".format(date))

payments_received_dfs_final.unpersist()
payments_received_dfs.unpersist()

# COMMAND ----------

if False in insert_passed_list:
  sys.exit("Error, payments_received data contained error!")

# COMMAND ----------


