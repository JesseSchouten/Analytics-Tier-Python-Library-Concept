# Databricks notebook source
import sys

# Python open source libraries
from datetime import datetime, timedelta, date
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, DecimalType, StructType, TimestampType, StructField
from pyspark.sql.functions import *

# Python DutchChannels libraries
from dc_azure_toolkit import sql
from data_sql.main import main as data_sql_main

# COMMAND ----------

START_DATE = str(datetime.now().date() - timedelta(days=378))
END_DATE = str(datetime.now().date() - timedelta(days=1))

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*4*2 

ENVIRONMENT = 'prd'

BATCH_SIZE = 30

REGISTER_LIST = ['2 - Registration', '2. Registration', '2 - Step 2: Registration', '2 - Registration', 'Smoketest - Register']
SUCCES_LIST = ['5 - Thank you', '5. Trial account' , '5 - Thank you' ,'5 - Step 6: Proefabonnee', 'Smoketest - Success']
PAYMENT_LIST = ['4 - Payment', '4. Pre-Adyen payment' , '4 - Payment', '4 - Step 4: Pre-Adyen payment pagina']
CHOOSE_PLAN_LIST = ['3 - Choose plan', '3. Choose plan', '3 - Choose plan', '3 - Step 3: Choose plan']
LANDING_LIST = ['1 - Landing', '1. Register form', '1 - Landing']

## keys are thighly coupled to code
COL_NAMES_DICT = {
  'landing' : 'landing',
  'register' : 'register',
  'choose_plan' : 'choose_plan',
  'payment' : 'payment',
  'succes' : 'succes' 
}

COLS_OF_INTEREST = [
    'date', 
    'account_id', 
    'property_id', 
    'property_name', 
    'view_id', 
    'view_name', 
    'view_timezone', 
    'channel', 
    'country',
    'segment', 
    'source', 
    'medium', 
    'campaign_name', 
    'ad_name',
    'sessions', 
    'new_users', 
    'pageviews', 
    'bounces',
    COL_NAMES_DICT['landing'],
    COL_NAMES_DICT['register'],
    COL_NAMES_DICT['choose_plan'],
    COL_NAMES_DICT['payment'],
    COL_NAMES_DICT['succes']
]

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

authentication_dict = ctx['SQLAuthenticator'].get_authentication_dict()

# COMMAND ----------

def log_start():
  print('starting to populate ga_daily_goals_report. Starting at: {} until: {}'.format(START_DATE, END_DATE))
  print('Using the following col names for the landing step: {}'.format(LANDING_LIST))
  print('Using the following col names for the register step: {}'.format(REGISTER_LIST))
  print('Using the following col names for the choose plan step: {}'.format(CHOOSE_PLAN_LIST))
  print('Using the following col names for the payment step: {}'.format(PAYMENT_LIST))
  print('Using the following col names for the succes step: {}'.format(SUCCES_LIST))
  print('Using the values of this dict as col names: {}'.format(COL_NAMES_DICT))

# COMMAND ----------

def get_start_tables():
    daily_ga_goals_report_dfs = sql.select_table("daily_ga_goals_report", authentication_dict, spark=spark)\
        .repartition(NR_PARTITIONS, "date", "account_id")\
        .cache()

    ga_custom_goals_dfs = sql.select_table("ga_custom_goals", authentication_dict, spark=spark) \
        .repartition(NR_PARTITIONS, "account_id") \
        .cache()
        
    return daily_ga_goals_report_dfs,ga_custom_goals_dfs

# COMMAND ----------

def rename_goal_to_generic(goal):
  if (any(goal.lower() in landing_goal_name.lower() for landing_goal_name in LANDING_LIST)): 
    return COL_NAMES_DICT["landing"] 
  elif (any(goal.lower() in register_goal_name.lower() for register_goal_name in REGISTER_LIST)):
    return COL_NAMES_DICT["register"]
  elif (any(goal.lower() in choose_plan_goal_name.lower() for choose_plan_goal_name in CHOOSE_PLAN_LIST)):
    return COL_NAMES_DICT["choose_plan"]
  elif (any(goal.lower() in payment_goal_name.lower() for payment_goal_name in PAYMENT_LIST)):
    return COL_NAMES_DICT["payment"] 
  elif (any(goal.lower() in succes_goal_name.lower() for succes_goal_name in SUCCES_LIST)):
    return COL_NAMES_DICT["succes"]
  else: 
    return "unknown_col"

# COMMAND ----------

def get_old_new_col_name_dict(ga_custom_goals_of_view_dfs, date):
  ga_custom_goals_on_date_for_view = ga_custom_goals_of_view_dfs.filter((col('start_date') <= given_date) & ((col('end_date').isNull()) | (col('end_date') >= given_date)))
  goal_index_keypair_dfs = ga_custom_goals_on_date_for_view.select(col('goals_index'), col('goals_name') )
  goal_index_keypair_dict = goal_index_keypair_dfs.rdd.collectAsMap()
  old_new_col_name_dict = {'goal_{}_completions'.format(goals_index):rename_goal_to_generic(goal_name) for goals_index,goal_name in goal_index_keypair_dict.items()}
  print('Date: {} \n Index and goal names: {} \n Old and new cols names: {}'.format(date ,goal_index_keypair_dict, old_new_col_name_dict))
  filtered_old_new_col_name_dict = {goals_index:goal_name for (goals_index,goal_name) in old_new_col_name_dict.items() if goal_name != 'unknown_col'}
  print('filtered {} unknown columns'.format(len(old_new_col_name_dict) - len(filtered_old_new_col_name_dict)))
  return filtered_old_new_col_name_dict

# COMMAND ----------

def rename_column_names(dfs, filtered_old_new_col_name_dict):
  for (old_col_name, new_col_name) in filtered_old_new_col_name_dict.items():
    dfs = dfs.withColumnRenamed(old_col_name, new_col_name)
  return dfs

# COMMAND ----------

def select_available_columns(dfs):
  columns_of_interest_in_dfs = [col for col in dfs.columns if col in COLS_OF_INTEREST]
  return dfs.select(*columns_of_interest_in_dfs)

# COMMAND ----------

def add_cols_not_in_dict(dfs, col_dict):
  for (key, col_name) in col_dict.items():
    if col_name not in dfs.columns:
      dfs = dfs.withColumn(col_name, lit(0))  
  return dfs

# COMMAND ----------

def select_cols_in_order(dfs):
  return dfs.select(*COLS_OF_INTEREST)

# COMMAND ----------

def create_ga_landing_page(daily_ga_goals_report_of_view_on_date_dfs, filtered_old_new_col_name_dict):
    daily_ga_goals_report_of_view_on_date_renamed_dfs = rename_column_names(
        daily_ga_goals_report_of_view_on_date_dfs, filtered_old_new_col_name_dict)
    ga_available_utm_cols_of_view_on_date_dfs = select_available_columns(
        daily_ga_goals_report_of_view_on_date_renamed_dfs)
    ga_processed_unordered_of_view_on_date_dfs = add_cols_not_in_dict(
        ga_available_utm_cols_of_view_on_date_dfs, COL_NAMES_DICT)
    ga_processed_of_view_on_date_dfs = select_cols_in_order(
        ga_processed_unordered_of_view_on_date_dfs)
    return ga_processed_of_view_on_date_dfs

# COMMAND ----------

counter = 0
ga_landing_page_service = ctx['GoogleAnalyticsLandingPageService']
ga_utm_processed_of_view_batched = None
batch_start_time = None
run_start_time = time.time()
log_start()

daily_ga_goals_report_dfs, ga_custom_goals_dfs = get_start_tables()

for view_id in daily_ga_goals_report_dfs.select('view_id').distinct().toPandas()['view_id'].tolist():
    if batch_start_time is None:
      batch_start_time = time.time()
      
    print('starting to process view with id: {}'.format(view_id))
    
    ga_custom_goals_of_view_dfs = ga_custom_goals_dfs.filter(col('view_id') == view_id)
    daily_ga_goals_report_of_view_dfs = daily_ga_goals_report_dfs.filter(col('view_id') == view_id)
    
    if isinstance(ga_custom_goals_of_view_dfs, type(None)) or isinstance(daily_ga_goals_report_of_view_dfs, type(None)):
      continue
    
    date_list = pd.date_range(start = START_DATE, end = END_DATE, freq ='D')
    for given_date in date_list:
      daily_ga_goals_report_of_view_on_date_dfs = daily_ga_goals_report_of_view_dfs.filter(col('date') == given_date)
      if isinstance(daily_ga_goals_report_of_view_on_date_dfs, type(None)):
        continue
      
      filtered_old_new_col_name_dict = get_old_new_col_name_dict(ga_custom_goals_of_view_dfs, given_date)    
      if isinstance(filtered_old_new_col_name_dict, type(None)):
        continue
      
      ga_processed_of_view_on_date_dfs = create_ga_landing_page(
            daily_ga_goals_report_of_view_on_date_dfs, filtered_old_new_col_name_dict)
      
      if isinstance(ga_processed_of_view_on_date_dfs, type(None)):
        continue
        
      counter += 1
      if isinstance(ga_utm_processed_of_view_batched, type(None)):
        ga_utm_processed_of_view_batched = ga_processed_of_view_on_date_dfs
      else:
        ga_utm_processed_of_view_batched = ga_utm_processed_of_view_batched.union(ga_processed_of_view_on_date_dfs).repartition(NR_PARTITIONS)
  
      if counter % (BATCH_SIZE) == 0:
        print("Finished a batch in %.2f s (inserted: %s records)" % (time.time() - batch_start_time, ga_utm_processed_of_view_batched.count()))
        ga_landing_page_service.run_insert(ga_utm_processed_of_view_batched)
        batch_start_time = None
        ga_utm_processed_of_view_batched = None

    # runs after date loop
    if not isinstance(ga_utm_processed_of_view_batched, type(None)):
      print("Finished final batch of view %s in %.2f s (inserted: %s records)" % (view_id, time.time() - batch_start_time, ga_utm_processed_of_view_batched.count()))
      ga_landing_page_service.run_insert(ga_utm_processed_of_view_batched)
      batch_start_time = None
      ga_utm_processed_of_view_batched = None

print('finished populating ga_processed_report. took: %.2f s' % (time.time() - run_start_time))
