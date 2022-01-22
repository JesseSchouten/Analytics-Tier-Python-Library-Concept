# Databricks notebook source
# Python standard library
import sys

# Python open source libraries
from datetime import datetime
import time
import pandas as pd
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# Python DutchChannels libraries
from dc_azure_toolkit import sql
from data_sql.main import main as data_sql_main

# COMMAND ----------

# Config for which DB we target, either dev or prd
ENVIRONMENT = 'prd'

# COMMAND ----------

ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)
authentication_dict = ctx['SQLAuthenticator'].get_authentication_dict()
subscriptions_dfs = sql.select_table("subscriptions", authentication_dict)

# COMMAND ----------

customer_state = subscriptions_dfs \
  .filter(col('state') != 'pending') \
  .groupBy('customer_id', 'channel').agg(min('subscription_create_date').alias('start'), \
                              max('subscription_end').alias('end'), \
                              max('recurring_enabled').alias('recurring')) \
  .withColumn('cohort', date_format(col('start'), 'yyyy-MM')) \
  .withColumn('age', \
              (date_format(col('end'), 'yyyy').cast(IntegerType()) - date_format(col('start'), 'yyyy').cast(IntegerType())) * 12 \
              + (date_format(col('end'), 'MM').cast(IntegerType()) - date_format(col('start'), 'MM').cast(IntegerType()))) \
  .withColumn('state', when(col('recurring') == True, lit('recurring')).otherwise(lit('cancelled')))

channel_histogram_alive = customer_state \
  .groupBy('channel', 'age').pivot('state').count().fillna(0) \
  .withColumn('total', col('recurring') + col('cancelled')) \
  .withColumn('older_than', sum(col('total')).over(Window.partitionBy('channel').orderBy(col('age').desc()).rowsBetween(-sys.maxsize, 0))) \
  .withColumn('churn', col('cancelled') / col('older_than')) \
  .withColumn('cohort', lit('')) \
  .select('channel', 'cohort', 'age', 'recurring', 'cancelled', 'total', 'older_than', 'churn')

cohort_histogram_alive = customer_state \
  .groupBy('channel', 'cohort', 'age').pivot('state').count().fillna(0) \
  .withColumn('total', col('recurring') + col('cancelled')) \
  .withColumn('older_than', sum(col('total')).over(Window.partitionBy('channel', 'cohort').orderBy(col('age').desc()).rowsBetween(-sys.maxsize, 0))) \
  .withColumn('churn', col('cancelled') / col('older_than')) \
  .select('channel', 'cohort', 'age', 'recurring', 'cancelled', 'total', 'older_than', 'churn')

histogram_alive = channel_histogram_alive.union(cohort_histogram_alive)

# COMMAND ----------

insert_passed = ctx['CustomerHistogramService'].run_insert(histogram_alive)

if insert_passed == False:
    sys.exit("Error, customer histogram data is not inserted!")
