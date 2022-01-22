# Databricks notebook source
# Python standard library
import sys
import re
import time
from datetime import datetime, timedelta, date

# Python open source libraries
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, DecimalType, StructType, TimestampType, StructField, DateType
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
import pytz

# Python DutchChannels libraries
from data_sql.main import main as data_sql_main
from dc_azure_toolkit import blob
from data_generic import country

# COMMAND ----------

START_DATE = str(datetime.now().date() - timedelta(days=370))
END_DATE = str(datetime.now().date() - timedelta(days=1))

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*4*2

# Number of days to group before inserting
BATCH_SIZE = 14

ENVIRONMENT = 'prd'

STORAGE_ACCOUNT_NAME = "udsprdsuppliersa"
CONTAINER_NAME = 'adverity'

# Mapping to point to file dir and name. {} gets filed with a date in yyyyMMdd format
PLATFORM_FILE_DICT = {
  'google-analytics-utm': 'google-analytics/daily-goals/utm/{}-google-analytics-daily-goals-utm-report.csv'
}

# When empty does nothing
COLS_WHEN_EMPTY_FILL_WITH = {
}

# When empty does nothing
COLS_WHEN_EMPTY_WILL_DROP_ROW = ['account_id']

# used to assign channel if lower(page exit path) or lower(property name) is in this list
LOV_LIST = ['withlove', 'with love']
NFN_LIST = ['newfaithnetwork', 'new faith network']
BTL_LIST = ['battlechannel', 'battle channel']
THR_LIST = ['thrilled']
UPL_LIST = ['uplifted']

COLS_TO_REPLACE_EMOTICONS_FROM = ['ad_name']
WORD_TO_REPLACE_EMOTICONS_WITH = 'emoticon '
COLS_TO_REMOVE_NON_ASCII_FROM = ['ad_name','campaign_name']
COLS_TO_LOWERCASE = ['ad_name','campaign_name']
COLS_TO_REMOVE_WHITESPACE = ['ad_name']

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)

# COMMAND ----------

# Configuration below tweaked based on "Learning Spark, 2nd edition, page 180".
spark = SparkSession \
    .builder \
    .appName("Customized spark session for daily_ga_goals_report notebook.") \
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

def get_dict_value(key, d):
  """
  Function to help safely retrieve a value from a dictionary based on an inputted key.
  Should not error, but retrieves an empty string instead.
  
  :param key: key to look up in the dictionary
  :param d: dictionary with pre-defined key-value pairs.
  :type key: str
  :type d: dict
  """
  try:
    return d[key]
  except Exception as e:
    return ""

def parse_date_to_blob_format(x):
  return str(x).replace('-','')

def get_dfs_by_file_and_date(file_path, date):
  authentication_dict = StorageAccountAuthenticator(STORAGE_ACCOUNT_NAME).get_authentication_dict()
  file_path = file_path.format(parse_date_to_blob_format(date.date()))
  print('getting file: '+ file_path)
  try:
    dfs = blob.select_from_csv(STORAGE_ACCOUNT_NAME, 
                               CONTAINER_NAME, 
                               authentication_dict, 
                               file_path, 
                               delimiter=';', 
                               spark=spark
                               )
  except Exception:
    print('unable to retrieve file: '+ file_path)
    return None
  return dfs

def get_daily_ga_utm_goals_dfs(date): 
  file_path = get_dict_value('google-analytics-utm', PLATFORM_FILE_DICT)
  dfs = get_dfs_by_file_and_date(file_path, date)
  return dfs

# COMMAND ----------

def drop_rows_when_na_in_col(dfs, col_names):
  if len(col_names) < 0:
    return dfs
  row_count_before = dfs.count()
  for col_name in col_names:
    dfs = dfs.na.drop(subset=[col_name])
    rows_droped = row_count_before - dfs.count()
    print('droped: {} rows due to na value in col: {}'.format(rows_droped, col_name))
  return dfs

# COMMAND ----------

def fill_rows_with_dict_value_when_na_in_col(dfs, cols_with_replace_value_dict):
  if len(cols_with_replace_value_dict) < 0:
    return dfs
  for col_name, replace_value in cols_with_replace_value_dict.items():
     dfs = dfs.na.fill(value=replace_value,subset=[col_name])
  return dfs

# COMMAND ----------

def add_channel_col_new(dfs):
  return dfs.withColumn('channel' 
                      ,when(eval(" | ".join(["lower(col('exit_page_path')).contains('{}')".format(channel_name) for channel_name in LOV_LIST])), 'lov')
                      .when(eval(" | ".join(["lower(col('exit_page_path')).contains('{}')".format(channel_name) for channel_name in NFN_LIST])), 'nfn')
                      .when(eval(" | ".join(["lower(col('exit_page_path')).contains('{}')".format(channel_name) for channel_name in BTL_LIST])), 'btl')
                      .when(eval(" | ".join(["lower(col('exit_page_path')).contains('{}')".format(channel_name) for channel_name in THR_LIST])), 'thr')
                      .when(eval(" | ".join(["lower(col('exit_page_path')).contains('{}')".format(channel_name) for channel_name in UPL_LIST])), 'thr')
                      .when(eval(" | ".join(["lower(col('property_name')).contains('{}')".format(channel_name) for channel_name in LOV_LIST])), 'lov')
                      .when(eval(" | ".join(["lower(col('property_name')).contains('{}')".format(channel_name) for channel_name in NFN_LIST])), 'nfn')
                      .when(eval(" | ".join(["lower(col('property_name')).contains('{}')".format(channel_name) for channel_name in BTL_LIST])), 'btl')
                      .when(eval(" | ".join(["lower(col('property_name')).contains('{}')".format(channel_name) for channel_name in THR_LIST])), 'thr')
                      .when(eval(" | ".join(["lower(col('property_name')).contains('{}')".format(channel_name) for channel_name in UPL_LIST])), 'thr')
                      .otherwise('nan'))

# COMMAND ----------

determine_country_udf = udf(lambda page_path, iso_country_code: country.determine_country(page_path, iso_country_code), StringType())

def add_country_col(dfs):
  dfs_with_country = dfs.withColumn('country', determine_country_udf(col('exit_page_path'), col('session_country_code')))
  return dfs_with_country

# COMMAND ----------

def select_to_length_and_cast_cols(dfs):
  return dfs.select(
    col('date').cast(DateType()),
    col('account_id').cast(IntegerType()),
    col('property_id'),
    col('property_name'),
    col('view_id').cast(IntegerType()),
    col('view_name'),
    col('view_timezone'),
    col('channel'),
    col('country'),
    substring(dfs.segment, 1, 100).alias("segment"),
    substring(dfs.source, 1, 60).alias("source"),
    substring(col("medium"), 1, 50).alias("medium"),
    substring(col("campaign_name"), 1, 173).alias("campaign_name"),
    substring(col("ad_name"), 1, 200).alias("ad_name"),
    col('sessions').cast(IntegerType()),
    col('new_users').cast(IntegerType()),
    col('pageviews').cast(IntegerType()),
    col('bounces').cast(IntegerType()),
    col('goal_1_completions').cast(IntegerType()),
    col('goal_2_completions').cast(IntegerType()),
    col('goal_3_completions').cast(IntegerType()),
    col('goal_4_completions').cast(IntegerType()),
    col('goal_5_completions').cast(IntegerType()),
    col('goal_6_completions').cast(IntegerType()),
    col('goal_7_completions').cast(IntegerType()),
    col('goal_8_completions').cast(IntegerType()),
    col('goal_9_completions').cast(IntegerType()),
    col('goal_10_completions').cast(IntegerType()),
    col('goal_11_completions').cast(IntegerType()),
    col('goal_12_completions').cast(IntegerType()),
    col('goal_13_completions').cast(IntegerType()),
    col('goal_14_completions').cast(IntegerType()),
    col('goal_15_completions').cast(IntegerType()),
    col('goal_16_completions').cast(IntegerType()),
    col('goal_17_completions').cast(IntegerType()),
    col('goal_18_completions').cast(IntegerType()),
    col('goal_19_completions').cast(IntegerType()),
    col('goal_20_completions').cast(IntegerType())
  )

# COMMAND ----------

def set_correct_length(dfs):
  return dfs\
  .withColumn('segment', col('segment').substr(1, 50))\
  .withColumn('source',col('source').substr(1, 60))\
  .withColumn('medium',col('medium').substr(1, 50))\
  .withColumn('campaign_name', col('campaign_name').substr(1, 110))\
  .withColumn('ad_name', col('ad_name').substr(1, 200))

# COMMAND ----------

def set_cols_to_lowercase(dfs, col_names):
  for col_name in col_names:
     dfs = dfs.withColumn(col_name, lower(col(col_name)))
  return dfs

# COMMAND ----------

def replace_emoticon_from_cols_with_word(dfs, col_names, word):
  for col_name in col_names:
    dfs = dfs.withColumn(col_name, regexp_replace(col(col_name), "[^\u0000-\u20CF]", word))
  return dfs

# COMMAND ----------

def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')

ascii_udf = udf(ascii_ignore)

def remove_non_ascii_from_cols(dfs, col_names):
    for col_name in col_names:
      dfs = dfs.withColumn(col_name, ascii_udf(col_name))
    return dfs

# COMMAND ----------

 def remove_white_space_from_cols(dfs, col_names):
  for col_name in col_names:
     dfs = dfs\
      .withColumn(col_name, ltrim(col(col_name)))\
      .withColumn(col_name, rtrim(col(col_name)))
  return dfs

# COMMAND ----------

def grouby_primary_keys(ga_goals_all_cols_dfs):
  return ga_goals_all_cols_dfs\
      .groupBy(['date', 'account_id', 'property_id', 'view_id','channel', 'country', 'segment', 'source', 'medium', 'campaign_name', 'ad_name'])\
      .agg(max('property_name').alias('property_name'),
           max('view_name').alias('view_name'),
           max('view_timezone').alias('view_timezone'),
           sum('sessions').alias('sessions'),
           sum('new_users').alias('new_users'),
           sum('pageviews').alias('pageviews'),
           sum('bounces').alias('bounces'),
           sum('goal_1_completions').alias('goal_1_completions'),
           sum('goal_2_completions').alias('goal_2_completions'),
           sum('goal_3_completions').alias('goal_3_completions'),
           sum('goal_4_completions').alias('goal_4_completions'),
           sum('goal_5_completions').alias('goal_5_completions'),
           sum('goal_6_completions').alias('goal_6_completions'),
           sum('goal_7_completions').alias('goal_7_completions'),
           sum('goal_8_completions').alias('goal_8_completions'),
           sum('goal_9_completions').alias('goal_9_completions'),
           sum('goal_10_completions').alias('goal_10_completions'),
           sum('goal_11_completions').alias('goal_11_completions'),
           sum('goal_12_completions').alias('goal_12_completions'),
           sum('goal_13_completions').alias('goal_13_completions'),
           sum('goal_14_completions').alias('goal_14_completions'),
           sum('goal_15_completions').alias('goal_15_completions'),
           sum('goal_16_completions').alias('goal_16_completions'),
           sum('goal_17_completions').alias('goal_17_completions'),
           sum('goal_18_completions').alias('goal_18_completions'),
           sum('goal_19_completions').alias('goal_19_completions'),
           sum('goal_20_completions').alias('goal_20_completions'))

# COMMAND ----------

def log_start():
  print('starting to populate ga_daily_goals_report. Starting at: {} until: {}'.format(START_DATE, END_DATE))
  print('filling rows when one of the following cols has a NA value: {}'.format(COLS_WHEN_EMPTY_FILL_WITH))
  print('droping rows when one of the following cols has a NA value: {}'.format(COLS_WHEN_EMPTY_WILL_DROP_ROW))
  print('repalcing emoticons in cols: {} with the following word: {}'.format(COLS_TO_REPLACE_EMOTICONS_FROM, WORD_TO_REPLACE_EMOTICONS_WITH))
  print('removing ascii from cols: {}'.format(COLS_TO_REMOVE_NON_ASCII_FROM))
  print('setting cols: {} to lowercase'.format(COLS_TO_LOWERCASE))
  print('whitespace from cols: {}'.format(COLS_TO_REMOVE_WHITESPACE))

# COMMAND ----------

def create_ga_table(raw_ga_utm_goals_dfs):
  ga_goals_filled_na_dfs = fill_rows_with_dict_value_when_na_in_col(raw_ga_utm_goals_dfs, COLS_WHEN_EMPTY_FILL_WITH)
  ga_goals_drop_na_dfs = drop_rows_when_na_in_col(ga_goals_filled_na_dfs, COLS_WHEN_EMPTY_WILL_DROP_ROW)
  ga_goals_with_channel_dfs = add_channel_col_new(ga_goals_drop_na_dfs)
  ga_goals_with_channel_country_dfs = add_country_col(ga_goals_with_channel_dfs)
  ga_goals_all_cols_dfs = select_to_length_and_cast_cols(ga_goals_with_channel_country_dfs)
  ga_goals_all_no_emoticons_dfs = replace_emoticon_from_cols_with_word(ga_goals_all_cols_dfs, COLS_TO_REPLACE_EMOTICONS_FROM, WORD_TO_REPLACE_EMOTICONS_WITH)
  ga_goals_all_cols_only_ascii_dfs = remove_non_ascii_from_cols(ga_goals_all_no_emoticons_dfs, COLS_TO_REMOVE_NON_ASCII_FROM)
  ga_goals_all_cols_lowered_dfs = set_cols_to_lowercase(ga_goals_all_cols_only_ascii_dfs, COLS_TO_LOWERCASE)
  ga_goals_all_cols_no_whitespace_dfs = remove_white_space_from_cols(ga_goals_all_cols_lowered_dfs, COLS_TO_REMOVE_WHITESPACE)
  ga_goals_grouped_dfs = grouby_primary_keys(ga_goals_all_cols_no_whitespace_dfs)
  return ga_goals_grouped_dfs

# COMMAND ----------

def insert_batched_dfs(ga_goals_batched_dfs, ga_daily_goals_service):
    ga_goals_batched_dfs = ga_goals_batched_dfs.cache()
    ga_daily_goals_service.run_insert(ga_goals_batched_dfs)
    ga_goals_batched_dfs.unpersist()
    print("Inserted untill date: {}".format(date))

# COMMAND ----------

date_list = pd.date_range(start = START_DATE, end = END_DATE, freq ='D')
ga_daily_goals_service = ctx['GoogleAnalyticsDailyGoalsService']
counter = 0
ga_goals_batched_dfs = None
batch_start_time = None
run_start_time = time.time()
log_start()
for date in date_list:
  ga_utm_goals_dfs = None
  
  if batch_start_time is None:
    batch_start_time = time.time()
  
  raw_ga_utm_goals_on_date_dfs = get_daily_ga_utm_goals_dfs(date)
  
  if isinstance(raw_ga_utm_goals_on_date_dfs, type(None)):
    continue
  
  ga_table_on_date_dfs = create_ga_table(raw_ga_utm_goals_on_date_dfs)
  
  counter += 1
  if isinstance(ga_goals_batched_dfs, type(None)):
    ga_goals_batched_dfs = ga_table_on_date_dfs
  else:
    ga_goals_batched_dfs = ga_goals_batched_dfs.union(ga_table_on_date_dfs).repartition(NR_PARTITIONS)
  
  if counter % (BATCH_SIZE) == 0:
    print("Duration %.2f s (found records: %s)" % (time.time() - batch_start_time, ga_goals_batched_dfs.count()))
    insert_batched_dfs(ga_goals_batched_dfs, ga_daily_goals_service)
    batch_start_time = None
    ga_goals_batched_dfs = None

if not isinstance(ga_goals_batched_dfs, type(None)):
  insert_batched_dfs(ga_goals_batched_dfs, ga_daily_goals_service)

raw_ga_utm_goals_on_date_dfs.unpersist()
ga_table_on_date_dfs.unpersist()

print('finished populating ga_daily_goals_report. took: %.2f s' % (time.time() - run_start_time))
