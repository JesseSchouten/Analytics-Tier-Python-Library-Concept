# Databricks notebook source
# Python DutchChannels libraries
from dc_azure_toolkit import cosmos_db
from data_sql.main import main as data_sql_main
# Python open source libraries
from datetime import datetime, timedelta
import time
import logging
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

ENVIRONMENT = 'prd'
MAX_DAYS_AGO_TO_GET_DATA_FROM = 5

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------


def select_correct_cols_and_set_types(wicketlabs_customers_dfs):
    return wicketlabs_customers_dfs.select(
        col("customerId").alias("customer_id"),
        col("subscriptionId").alias("subscription_Id"),
        col("activeStartDate").cast(DateType()).alias("active_start_date"),
        col("trialStartDate").cast(DateType()).alias("trial_start_date"),
        col("activeEndDate").cast(DateType()).alias("active_end_date"),
        col("channel"),
        col("productName").alias("product_name"),
        col("isReconnect").cast(BooleanType()).alias("is_reconnect"),
        col("usedSpecialOffer").cast(
            BooleanType()).alias("used_special_offer"),
        col("subscriptionUnit").cast(DoubleType()).alias("subscription_unit"),
        col("unitPrice").cast(DoubleType()).alias("unit_price"),
        col("unitPriceCurrency").alias("unit_price_currency"),
        col("totalpayments").cast(DoubleType()).alias("total_payments"),
        col("lastviewdate").cast(DateType()).alias("last_view_date"),
        col("dayssincelastengagement").cast(
            IntegerType()).alias("days_since_last_engagement"),
        col("tenure").cast(IntegerType()),
        col("engagementstatus").alias("engagement_status"),
        col("primarydevicelifetime").alias("primary_device_life_time"),
        col("primarydevicelastthreemonths").alias(
            "primary_device_last_three_months"),
        col("primaryviewinglocationlifetime").alias(
            "primary_viewing_location_life_time"),
        col("primaryviewinglocationlastthreemonths").alias(
            'primary_viewing_location_last_three_months'),
        col("hoursviewedlastthreemonths").cast(
            DoubleType()).alias("hours_viewed_last_three_months"),
        col("customersubscriptioncount").cast(
            IntegerType()).alias("customer_subscription_count"),
        col("customertenure").cast(IntegerType()).alias("customer_tenure"),
        col("customervoluntarycancellationcount").cast(
            IntegerType()).alias("customer_voluntary_cancellation_count"),
        col("customerhoursviewed").cast(
            DoubleType()).alias("customer_hours_viewed"),
        col("entertainmentindexlastthreemonths").cast(
            DoubleType()).alias("entertainment_index_last_three_months"),
        col("entertainmentindexlifetime").cast(
            DoubleType()).alias("entertainment_index_lifetime"),
        col("date").cast(DateType()),
        col("id")
    )

# COMMAND ----------

def set_bit_values(wicketlabs_customers_unique_customer_id_selected_and_typed_dfs):
    return wicketlabs_customers_unique_customer_id_selected_and_typed_dfs\
        .withColumn('is_reconnect', when((col('is_reconnect') == 'true'), lit(1)).otherwise(lit(0)))\
        .withColumn('used_special_offer', when((col('used_special_offer') == 'true'), lit(1)).otherwise(lit(0)))

# COMMAND ----------


def get_select_query_for_single_day(start_date, step_to_next_day):
    if (start_date is None or step_to_next_day is None):
        return
    min_date = (start_date + timedelta(days=step_to_next_day)
                ).strftime("%Y-%m-%d")
    max_date = (start_date + timedelta(days=step_to_next_day+1)
                ).strftime("%Y-%m-%d")
    query = f'SELECT * FROM c WHERE c.date>"{min_date}" and c.date<"{max_date}"'
    return query

# COMMAND ----------


def remove_duplicate_customers_by_max_view_date(wicketlabs_customers_dfs):
    return wicketlabs_customers_dfs \
        .withColumn("max_last_view_date", max("lastViewDate").over(Window.partitionBy("customerId"))) \
        .filter(col("lastViewDate") == col("max_last_view_date")) \
        .drop_duplicates(["customerId"])

# COMMAND ----------


auth_dict = CosmosAuthenticator('uds-prd-db-account').get_authentication_dict()
ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)
wicketlabs_customers_service = ctx['WicketlabsCustomersService']
start_date = (datetime.now() - timedelta(days=MAX_DAYS_AGO_TO_GET_DATA_FROM))
i = 0
while i <= MAX_DAYS_AGO_TO_GET_DATA_FROM:
    query_for_single_day = get_select_query_for_single_day(start_date, i)
    if query_for_single_day:
        wicketlabs_customers_for_given_day_dfs = cosmos_db.select(
            query_for_single_day, auth_dict, 'uds-db', 'wicketlabs-customers', 'dfs')
        wicketlabs_customers_unique_customer_id_dfs = remove_duplicate_customers_by_max_view_date(
            wicketlabs_customers_for_given_day_dfs)
        wicketlabs_customers_unique_customer_id_selected_and_typed_dfs = select_correct_cols_and_set_types(
            wicketlabs_customers_unique_customer_id_dfs)
        wicketlabs_customers_final = set_bit_values(
            wicketlabs_customers_unique_customer_id_selected_and_typed_dfs)
        wicketlabs_customers_service.run_insert(
            wicketlabs_customers_final)
    i += 1
del auth_dict, query_for_single_day
