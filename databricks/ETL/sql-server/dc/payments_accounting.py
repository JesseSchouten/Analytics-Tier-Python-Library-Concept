# Databricks notebook source
# Python standard library
import sys
import re
import math

# Python open source libraries
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql import SparkSession
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

# Retrieve data from NUMBER_OF_DAYS number of days, taking the START_DATE as starting point, writing to the db every DAYS_BATCH_SIZE days
NUMBER_OF_DAYS = 100
START_DATE = datetime.now().date() - timedelta(days=NUMBER_OF_DAYS)
DAYS_BATCH_SIZE = 100

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

def get_payments_accounting_dfs():
  """
  Function to retrieve the payments_accounting reports.
  Currently stored csv's on the dcinternal storage account.
  """
  storage_account_name = "dcinternal"
  authentication_dict = StorageAccountAuthenticator(storage_account_name).get_authentication_dict()

  file_path = 'adyen/reports/transformed/payments_accounting_report.csv'
  channel_list = ['nfn', 'lov']
    
  payments_accounting_dfs = None
  for channel in channel_list:
    container_name = "dc" + channel
    if isinstance(payments_accounting_dfs, type(None)):
      payments_accounting_dfs = blob.select_from_csv(storage_account_name, container_name, authentication_dict, file_path)
    else:
      payments_accounting_dfs = payments_accounting_dfs.union(blob.select_from_csv(storage_account_name, container_name, authentication_dict, file_path))
  
  payments_accounting_dfs = payments_accounting_dfs
  return payments_accounting_dfs.repartition(NR_PARTITIONS)

def preprocess_payments_accounting_dfs(payments_accounting_dfs):
    """
    Preprocessing steps we want to apply on all of the data, rather then on a per-day basis.
    
    Steps:
    - select and ordering the columns.
    - imputing null values with 0 for the processing_fee_fc column. Necassary as it is a key in our db for now.
    - parse booking_date to timestamp.
    - Drop all duplicate records, which can be caused due to us neglecting some unimportant columns in step 1.
    """
    payments_accounting_dfs_final = payments_accounting_dfs.select(col("Company Account").alias('company_account'),
                                    col("Merchant Account").alias('merchant_account'),
                                    col("Psp Reference").alias('psp_reference'),
                                    col("Merchant Reference").alias('merchant_reference'),
                                    col("Payment Method").alias('payment_method'),
                                    col("Booking Date").alias('booking_date'),
                                    col("TimeZone").alias('time_zone'),
                                    col("Main Currency").alias('main_currency'),
                                    col("Main Amount").alias('main_amount'),
                                    col("Record Type14").alias('record_type'),
                                    col("Payment Currency").alias('payment_currency'),
                                    col("Received (PC)").alias('received_pc'),
                                    col("Authorised (PC)").alias('authorized_pc'),
                                    col("Captured (PC)").alias('captured_pc'),
                                    col("Settlement Currency").alias('settlement_currency'),
                                    col("Payable (SC)").alias('payable_sc'),
                                    col("Commission (SC)").alias('commission_sc'),
                                    col("Markup (SC)").alias('markup_sc'),
                                    col("Scheme Fees (SC)").alias('scheme_fees_sc'),
                                    col("Interchange (SC)").alias('interchange_sc'),
                                    col("Processing Fee Currency").alias('processing_fee_currency'),
                                    col("Processing Fee (FC)").alias('processing_fee_fc'),
                                    col("Payment Method Variant").alias('payment_method_variant'))   
    
    payments_accounting_dfs_final = payments_accounting_dfs_final.na.fill(value=0,subset=["processing_fee_fc"])
    payments_accounting_dfs_final = payments_accounting_dfs_final.withColumn('booking_date', payments_accounting_dfs_final['booking_date'].cast(TimestampType()))
    payments_accounting_dfs_final = payments_accounting_dfs_final.dropDuplicates()
    
    return payments_accounting_dfs_final  

payments_accounting_dfs = get_payments_accounting_dfs()
payments_accounting_dfs = preprocess_payments_accounting_dfs(payments_accounting_dfs).cache()

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
    result = ''
  
  return result

def create_payments_accounting_report_between_dates(start, end):
  """
  
  """
  payments_accounting_dfs_final = payments_accounting_dfs.filter((col('booking_date') >= start) & (col('booking_date') < end))
  MerchantReference = udf(lambda x: parse_uuid(x), StringType())
  payments_accounting_dfs_final = payments_accounting_dfs_final.withColumn("merchant_reference", MerchantReference("merchant_reference"))
  
  return payments_accounting_dfs_final

# COMMAND ----------

date_list = pd.date_range(start = START_DATE, end = START_DATE + pd.DateOffset(days=NUMBER_OF_DAYS), freq ='D')

nr_iterations_to_completion = math.ceil(NUMBER_OF_DAYS / DAYS_BATCH_SIZE)

daily_report_final_dfs = None
insert_passed_list = []
for day_nr in range(0, nr_iterations_to_completion):
  timer = time.time()
  
  current_date_range = date_list[day_nr * DAYS_BATCH_SIZE: (day_nr + 1) * DAYS_BATCH_SIZE]
  start_date = current_date_range[0]
  end_date = current_date_range[-1] + timedelta(days=1)
  
  payments_accounting_dfs_final = create_payments_accounting_report_between_dates(start_date, end_date).cache()

  insert_passed = ctx['PaymentsAccountingService'].run_insert(payments_accounting_dfs_final)
  insert_passed_list.append(insert_passed)
    
  payments_accounting_dfs_final.unpersist()
  
  print("Start: {}, End: {}".format(start_date, end_date))
  print("Duration {:.2f} s (found records: {})".format(time.time() - timer, payments_accounting_dfs_final.count()))
  
payments_accounting_dfs.unpersist()

# COMMAND ----------

if False in insert_passed_list:
  sys.exit("Error, Payments Accounting data is not inserted!")
