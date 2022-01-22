# Databricks notebook source
# Python standard library
import sys
from functools import reduce  

# Python open source libraries
from datetime import datetime, timedelta
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, DecimalType, StructType, TimestampType, StructField
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
import pytz

# Python DutchChannels libraries
from data_sql.main import main as data_sql_main
from dc_azure_toolkit import blob

# COMMAND ----------

START_DATE = str(datetime.now().date() - timedelta(days=15))
END_DATE = str(datetime.now().date() - timedelta(days=0))
# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*4*2 

ENVIRONMENT = 'prd'

STORAGE_ACCOUNT_NAME = "udsprdsuppliersa"

BATCH_SIZE = 7

# Mapping objects to be manually configured.
PLATFORM_FILE_DICT = {
  'facebook': 'facebook-ads/daily-ad/{}-facebook-ads-daily-ad-country-report.csv',
  'google-360': 'google-360/hourly-ad/{}-google-360-hourly-ad-report.csv',
  'google-ads': 'google-ads/daily-adset/{}-google-ads-daily-adset-report.csv',
  'pinterest': 'pinterest/hourly-ad/{}-pinterest-hourly-ad-report.csv',
  'snapchat': 'snapchat/daily-ad/{}-snapchat-daily-ad-report.csv',
  'microsoft-ads':'microsoft-ads/hourly-adset/{}-microsoft-ads-hourly-adset-country-report.csv'
}

SNAPCHAT_ACCOUNT_TO_CHANNEL = {
  'In Love Media B.V. Self Service': 'lov',
  'NFN Media BV Self Service': 'nfn'
}

PINTEREST_ADS_ACCOUNT_TO_CHANNEL = {
  'NFN Media BV': 'nfn', 
  'In Love Media B.V.': 'lov'
}

FACEBOOK_ADS_ACCOUNT_TO_CHANNEL = {
  'WithLove Ads': 'lov', 
  'New Faith Network NL Ads': 'nfn', 
  'Smoketest': 'st'
}

GOOGLE_360_ADS_ACCOUNT_TO_CHANNEL = {
  'DutchChannels': 'lov'
}

GOOGLE_ADS_ACCOUNT_TO_CHANNEL = {
  'New Faith Network AUS': 'nfn',
  'New Faith Network IE': 'nfn',
  'New Faith Network New Zealand':'nfn',
  'New Faith Network NL': 'nfn',
  'New Faith Network NO': 'nfn',
  'New Faith Network SE': 'nfn',
  'New Faith Network UK': 'nfn',
  'New Faith Network INT': 'nfn',
  'With Love': 'lov',
  'With Love NO': 'lov',
  'With Love SE': 'lov',
  'With Love INT': 'lov',
  'With Love BE': 'lov',
  'Thrilled': 'st',
  'BattleChannel': 'st',
  'Uplifted': 'st'
}

GOOGLE_ADS_ACCOUNT_TO_COUNTRY = {
  'New Faith Network AUS': 'AU',
  'New Faith Network IE': 'GB',
  'New Faith Network New Zealand':'NZ',
  'New Faith Network NL': 'NL',
  'New Faith Network NO': 'NO',
  'New Faith Network SE': 'SE',
  'New Faith Network UK': 'GB',
  'New Faith Network INT': '',
  'With Love': 'NL',
  'With Love NO': 'NO',
  'With Love SE': 'SE',
  'With Love INT': '',
  'With Love BE': 'BE',
  'Thrilled': '',
  'BattleChannel': '',
  'Uplifted': ''
}

MICROSOFT_ADS_ACCOUNT_TO_CHANNEL = {
  'New Faith Network AUS': 'nfn',
  'New Faith Network IE': 'nfn',
  'New Faith Network NZ':'nfn',
  'New Faith Network NL': 'nfn',
  'New Faith Network NO': 'nfn',
  'New Faith Network SE': 'nfn',
  'New Faith Network UK': 'nfn',
  'WithLove': 'lov',
  'WithLove Norway': 'lov',
  'WithLove Sweden': 'lov', 
  'WithLove Belgie': 'lov', 
}

COUNTRY_MAPPER = {
  'United Kingdom': 'GB',
  'Sweden': 'SE',
  'Netherlands': 'NL',
  'Belgium': 'BE',
  'Norway': 'NO', 
  'New Zealand': 'NZ',
  'Australia': 'AU'
}

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

# COMMAND ----------

def create_base_dfs():
  schema = StructType([ \
      StructField("date",TimestampType(),True), \
      StructField("channel",StringType(),True), \
      StructField("country",StringType(),True), \
      StructField("social_channel", StringType(), True), \
      StructField("account_id", StringType(), True), \
      StructField("account_name", StringType(), True), \
      StructField("campaign_id",StringType(),True), \
      StructField("campaign_name",StringType(),True), \
      StructField("adset_id",StringType(),True), \
      StructField("adset_name", StringType(), True), \
      StructField("ad_id", StringType(), True), \
      StructField("ad_name", StringType(), True), \
      StructField("time_zone",StringType(),True), \
      StructField("currency",StringType(),True), \
      StructField("paid_impressions",IntegerType(),True), \
      StructField("paid_clicks", IntegerType(), True), \
      StructField("costs", DecimalType(29,2), True), \
    ])
  dfs = spark.createDataFrame(data=[],schema=schema)
  return dfs

def unionAll(dfs_list):
  return reduce(DataFrame.unionAll, tuple(dfs_list))  

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
  
def concat_date_and_hour(x, y):
  return x + timedelta(hours=y)

def group_to_daily(dfs):
  dfs = dfs.select("date", 'channel', 'country', 'social_channel', \
               'account_id', 'account_name', 'campaign_id', 'campaign_name', \
               'adset_id', 'adset_name', 'ad_id', 'ad_name', 'time_zone', \
               'currency', 'paid_impressions','paid_clicks', 'costs') \
       .groupBy("date", 'channel', 'country', 'social_channel', \
                'account_id', 'account_name', 'campaign_id', \
                'campaign_name', 'adset_id', 'adset_name', 'ad_id',\
                'ad_name', 'time_zone', 'currency') \
       .agg(sum('paid_impressions').alias('paid_impressions'), \
             sum('paid_clicks').alias('paid_clicks'), \
             sum('costs').alias('costs'))
  return dfs

def reorder_columns(dfs):
  dfs = dfs.select("date", 'channel', 'country', 'social_channel', \
                   'account_id', 'account_name', 'campaign_id', \
                   'campaign_name', 'adset_id', 'adset_name', 'ad_id', \
                   'ad_name', 'time_zone', 'currency', 'paid_impressions', \
                   'paid_clicks', 'costs')
  return dfs

def parse_date_to_blob_format(x):
  return str(x).replace('-','')

# COMMAND ----------

def add_customized_columns_fb(dfs):
  ChannelUDF = udf(lambda x:get_dict_value(x, FACEBOOK_ADS_ACCOUNT_TO_CHANNEL), StringType())
  dfs = dfs.withColumn("social_channel", lit("facebook"))
  dfs = dfs.withColumn("channel", ChannelUDF("account_name"))
  return dfs

def add_customized_columns_google_360(dfs):
  ChannelUDF = udf(lambda x: get_dict_value(x, GOOGLE_360_ADS_ACCOUNT_TO_CHANNEL), StringType())
  dfs = dfs.withColumn("social_channel", lit("google-360"))
  dfs = dfs.withColumn("channel", ChannelUDF("account_name"))
  return dfs

def add_customized_columns_google_ads(dfs):
  ChannelUDF = udf(lambda x: get_dict_value(x, GOOGLE_ADS_ACCOUNT_TO_CHANNEL), StringType())
  CountryUDF = udf(lambda x: get_dict_value(x, GOOGLE_ADS_ACCOUNT_TO_COUNTRY), StringType())
  dfs = dfs.withColumn('ad_id', lit(""))
  dfs = dfs.withColumn('ad_name', lit(""))
  dfs = dfs.withColumn("channel", ChannelUDF("account_name"))
  dfs = dfs.withColumn("country", CountryUDF("account_name"))
  dfs = dfs.withColumn("social_channel", lit("google-ads"))
  
  return dfs

def add_customized_columns_pinterest(dfs):
  ChannelUDF = udf(lambda x: get_dict_value(x, PINTEREST_ADS_ACCOUNT_TO_CHANNEL), StringType())
  dfs = dfs.withColumn('time_zone', lit('UTC'))
  dfs = dfs.withColumn("channel", ChannelUDF("account_name"))
  dfs = dfs.withColumn("social_channel", lit("pinterest"))
  
  return dfs

def add_customized_columns_snapchat(dfs):
  ChannelUDF = udf(lambda x:get_dict_value(x, SNAPCHAT_ACCOUNT_TO_CHANNEL), StringType())
  dfs = dfs.withColumn("social_channel", lit("snapchat"))
  dfs = dfs.withColumn("channel", ChannelUDF("account_name"))
  return dfs

def add_customized_columns_microsoft_ads(dfs):
  ChannelUDF = udf(lambda x:get_dict_value(x, MICROSOFT_ADS_ACCOUNT_TO_CHANNEL), StringType())
  dfs = dfs.withColumn('time_zone', lit('Europe/Amsterdam'))
  dfs = dfs.withColumn('ad_id', lit(""))
  dfs = dfs.withColumn('ad_name', lit(""))
  dfs = dfs.withColumn("channel", ChannelUDF("account_name"))
  dfs = dfs.withColumn("social_channel", lit("microsoft-ads"))
  return dfs

def process_columns_fb(dfs):
  dfs = dfs.withColumnRenamed("country_code", "country")
  dfs = dfs.withColumnRenamed("account_timezone", "time_zone")
  dfs = dfs.withColumn('country', upper(dfs.country))
  dfs = dfs.withColumn('country',
                       when(dfs.country == 'UNKNOWN', "") \
                       .otherwise(dfs.country)
                      )
  
  dfs = reorder_columns(dfs)
  return dfs

def process_columns_google_360(dfs):
  dfs = dfs.withColumnRenamed("account_timezone","time_zone")
  dfs = dfs.withColumnRenamed("country_code","country")
  dfs = dfs.withColumn('country', upper(dfs.country))
  dfs = dfs.withColumn('country',
                     when(dfs.country == 'UNKNOWN', "") \
                     .otherwise(dfs.country)
                    )
  
  dfs = group_to_daily(dfs)
  
  dfs = reorder_columns(dfs)
  
  return dfs

def process_columns_google_ads(dfs, **kwargs):
  dfs = dfs.withColumnRenamed("account_timezone","time_zone")
  
  dfs = reorder_columns(dfs)
  return dfs


def process_columns_pinterest(dfs):
  DateHourUDF = udf(concat_date_and_hour, TimestampType())
  
  # deliberately write the paid impressions and subs to the db.
  dfs = dfs.withColumnRenamed('country_code', 'country')
  
  # convert from utc to amsterdam time
    # we NEED the files of the day before and day after to make this work
  dfs = dfs.withColumnRenamed('hour', 'hour_utc')
  dfs = dfs.withColumnRenamed('date', 'date_utc')
  dfs = dfs.withColumn('date_utc', dfs.date_utc.cast('timestamp'))
  dfs = dfs.withColumn('date_utc', DateHourUDF("date_utc", "hour_utc"))
  dfs = dfs.withColumn("date", from_utc_timestamp(col("date_utc"), 'Europe/Amsterdam'))
  dfs = dfs.withColumn('time_zone', lit('Europe/Amsterdam'))
  dfs = dfs.withColumn('date', to_date(col("date"))) 
  
  dfs = group_to_daily(dfs)
  
  # Added because of a weird instance where the ad_id had more then 1 name. In that case: take a random name. 
  dfs = dfs.select("date", 'channel', 'country', 'social_channel', \
              'account_id', 'account_name', 'campaign_id', \
              'campaign_name', 'adset_id', 'adset_name', 'ad_id', \
              'ad_name', 'time_zone', 'currency', 'paid_impressions', \
              'paid_clicks', 'costs') \
     .groupBy("date", 'channel', 'country', 'social_channel', \
              'account_id', 'account_name', 'campaign_id', \
              'campaign_name', 'adset_id', 'adset_name', 'ad_id',\
              'time_zone', 'currency') \
     .agg(sum('paid_impressions').alias('paid_impressions'), \
           sum('paid_clicks').alias('paid_clicks'), \
           sum('costs').alias('costs'), \
           max('ad_name').alias('ad_name'))
  
  dfs = reorder_columns(dfs)
  return dfs

def process_columns_snapchat(dfs):
  dfs = dfs.withColumnRenamed("account_timezone","time_zone")
  dfs = dfs.withColumnRenamed("country_code","country")
  dfs = dfs.withColumn("country",upper('country'))
  dfs = dfs.withColumnRenamed("account_timezone", "time_zone")
  
  dfs = reorder_columns(dfs)
  return dfs

def process_columns_microsoft_ads(dfs):
  CountryUDF = udf(lambda x:get_dict_value(x, COUNTRY_MAPPER), StringType())
  dfs = dfs.withColumn("country_name", CountryUDF('country_name'))
  dfs = dfs.withColumnRenamed("country_name","country")
  
  dfs = group_to_daily(dfs)
  
  dfs = reorder_columns(dfs)
  return dfs

def add_customized_columns(social_channel, dfs):
  if social_channel == 'google-ads':
    dfs = add_customized_columns_google_ads(dfs)
  elif social_channel == 'google-360':
    dfs = add_customized_columns_google_360(dfs)
  elif social_channel == 'facebook':
    dfs = add_customized_columns_fb(dfs)
  elif social_channel == 'pinterest':
    dfs = add_customized_columns_pinterest(dfs)
  elif social_channel == 'snapchat':
    dfs = add_customized_columns_snapchat(dfs)
  elif social_channel == 'microsoft-ads':
    dfs = add_customized_columns_microsoft_ads(dfs)
  return dfs

def process_columns(social_channel, dfs):
  if social_channel == 'google-ads':
    dfs = process_columns_google_ads(dfs)
  elif social_channel == 'google-360':
    dfs = process_columns_google_360(dfs)
  elif social_channel == 'facebook':
    dfs = process_columns_fb(dfs)
  elif social_channel == 'pinterest':
    dfs = process_columns_pinterest(dfs)
  elif social_channel == 'snapchat':
    dfs = process_columns_snapchat(dfs)
  elif social_channel == 'microsoft-ads':
    dfs = process_columns_microsoft_ads(dfs)
  return dfs  

def get_social_channel_dfs(social_channel, date, storage_account_name, authentication_dict, container_name):
  
  file_path = PLATFORM_FILE_DICT[social_channel]
  try:
      dfs = blob.select_from_csv(storage_account_name \
                                  , container_name \
                                  , authentication_dict \
                                  , file_path.format(parse_date_to_blob_format(date.date())) \
                                  , delimiter=';' \
                                  , spark=spark \
                                 ) 
      # We assume that we can only encounter timezones of above UTC, therefore if we want to convert
      # we only need data from the previous day in the shifting process.
      if social_channel == 'pinterest':
        dfs = dfs.union(blob.select_from_csv(storage_account_name \
                                  , container_name \
                                  , authentication_dict \
                                  , file_path.format(parse_date_to_blob_format(date.date() - timedelta(days=1))) \
                                  , delimiter=';' \
                                  , spark=spark
                                 ))
  except AnalysisException:
    return create_base_dfs()
  
  credentials = {'social_channel': social_channel, \
                 'file_path': file_path, \
                 'storage_account_name':storage_account_name, \
                 'container_name':container_name, \
                 'authentication_dict': authentication_dict, \
                 'date': date
                }
  
  dfs = add_customized_columns(social_channel, dfs)
  dfs = process_columns(social_channel, dfs)
  
  dfs = dfs.fillna(value=0, subset=["paid_impressions", 'paid_clicks', 'costs'])
  
  dfs = dfs.filter((dfs.date >= date) & (dfs.date < date + timedelta(days=1)))
  
  return dfs

def get_daily_socials_dfs(date): 
  authentication_dict = StorageAccountAuthenticator(STORAGE_ACCOUNT_NAME).get_authentication_dict()
  container_name = 'adverity'

  social_channels = ['google-360', 'facebook', 'google-ads', 'pinterest', 'snapchat', 'microsoft-ads']
  
  dfs = create_base_dfs()
  for social_channel in social_channels:
    dfs = dfs.union(get_social_channel_dfs(social_channel, date, STORAGE_ACCOUNT_NAME, authentication_dict, container_name))
    
  return dfs

# COMMAND ----------

date_list = pd.date_range(start = START_DATE, end = END_DATE, freq ='D')

insert_passed_list = []
daily_dfs_list = []
for date in date_list:
  timer = time.time()
  
  date_string = str(date.date())
  
  daily_socials_dfs = get_daily_socials_dfs(date)
  daily_dfs_list.append(daily_socials_dfs)
  
  if len(daily_dfs_list) == BATCH_SIZE:
    daily_socials_final_dfs = unionAll(daily_dfs_list).repartition(NR_PARTITIONS)
    insert_passed = ctx['DailyAdReportService'].run_insert(daily_socials_final_dfs)
    insert_passed_list.append(insert_passed)  
    daily_dfs_list.clear()
    print("Inserted untill date: {}".format(date))
    
  print("Duration %.2f s (found records: %s)" % (time.time() - timer, daily_socials_dfs.count()))
  
if len(daily_dfs_list) > 0:
  daily_socials_final_dfs = unionAll(daily_dfs_list).repartition(NR_PARTITIONS)
  insert_passed = ctx['DailyAdReportService'].run_insert(daily_socials_final_dfs)
  insert_passed_list.append(insert_passed)
  
  print("Inserted untill date: {}".format(date))

daily_socials_final_dfs.unpersist()
daily_socials_dfs.unpersist()

# COMMAND ----------

if False in insert_passed_list:
  sys.exit("Error, daily_ad_report data contained error!")
