# Databricks notebook source
# Python standard library
import sys

# Python open source libraries
from datetime import datetime, timedelta
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, DecimalType
from pyspark.sql.functions import *

# Python DutchChannels libraries
from dc_azure_toolkit import sql
from data_sql.main import main as data_sql_main

# COMMAND ----------

# Hardcoded string which represents the start of DutchChannels
START = '2017-12-01'

# Config for which DB we target, either dev or prd
ENVIRONMENT = 'prd'

# Number of cores per worker * Number of workers * 2; use at least as many partitions as cores present
NR_PARTITIONS = 8*6*2 

MERCHANT_ACCOUNT_MAPPER = {
  "WithLove": 'lov',
  "DutchChannelsBVNL": 'nfn'
}

# COMMAND ----------

# Configuration below tweaked based on "Learning Spark, 2nd edition, page 180".
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
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
  .repartition(NR_PARTITIONS, "customer_id") \
  .withColumnRenamed('first_payment_date', 'customer_first_payment_date') \
  .cache()

subscriptions_dfs = sql.select_table("subscriptions", authentication_dict, spark=spark) \
  .repartition(NR_PARTITIONS, "customer_id") \
  .cache()

payments_dfs = sql.select_table("payments", authentication_dict, spark=spark) \
  .repartition(NR_PARTITIONS, "merchant_reference") \
  .cache()

payments_info_dfs = sql.select_table("payments_info", authentication_dict, spark=spark) \
  .repartition(NR_PARTITIONS, "merchant_reference") \
  .cache()

# COMMAND ----------

def map_channel(merchant_account):
  """
  :param merchant_account: merchant_account
  :type merchant_account: str
  
  :returns: channel belonging to this merchant: str
  """  
  try:
    return MERCHANT_ACCOUNT_MAPPER[merchant_account]
  except Exception:
    return ''

def add_channel(channel, merchant_account):
  """
  Written to extend the channel for payments without a subscription_id (InvoiceDeduction, BalanceTransfer, etc).
  The idea is to get the channel based on the corresponding merchant_ref. If not possible, take the channel based on 
  the merchant_account.
  
  :param channel: channel
  :param merchant_account: merchant_account
  
  :type channel: str
  :type merchant_account: str
  
  :returns: channel if filled otherwise the channel belonging to this merchant: str
  """
  
  if isinstance(channel, type(None)) or channel == '':
    return map_channel(merchant_account)
  else: 
    return channel

# COMMAND ----------

def revenue_per_type_between_dates(start, end):
  
  subscriptions = subscriptions_dfs \
                  .filter(col('state') != 'pending') \
                  .filter(col('subscription_create_date') < end) \
                  .select("subscription_id", 'channel', 'country') \
                  .fillna("", ['channel', 'country'])
  
  get_channel_udf = udf(add_channel, StringType())
  payments_info = payments_info_dfs \
                  .filter((col('payment_received_date') < end)) \
                  .join(subscriptions.select("subscription_id", "channel", 'country'), col('merchant_reference') == col('subscription_id'), 'leftouter') \
                  .withColumn("channel", get_channel_udf("channel", "merchant_account")) \
                  .fillna("", ['channel', 'country']) \
                  .select('channel', 'country', 'type', 'gross_debit', 'gross_credit', 'net_debit', 'net_credit', 'commission', 'markup', 'scheme_fees', 'interchange', 'exchange_rate') \
                  .filter(col('payment_received_date') >= start) \
                  .fillna(0) \
                  .withColumn("gross_credit_exchanged", expr("gross_credit * exchange_rate")) \
                  .withColumn("gross_debit_exchanged", expr("gross_debit * exchange_rate"))
  
  # total
  total_cols = payments_info \
                      .filter((col('type') != 'MerchantPayout')) \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('net_credit').alias('sum_net_credit'), \
                           sum('net_debit').alias('sum_net_debit')) \
                      .withColumn("sum_of_net_payout", expr("sum_net_credit - sum_net_debit")) \
                      .withColumn('month', lit(start))
  # settled
  settled_cols = payments_info \
                      .filter(col('type') == 'Settled') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('gross_credit_exchanged').alias('sum_gross_credit_exchanged'), \
                           sum('gross_debit_exchanged').alias('sum_gross_debit_exchanged'), \
                           sum('commission').alias('commission'), \
                           sum('markup').alias('markup'), \
                           sum('scheme_fees').alias('scheme_fees'), \
                           sum('interchange').alias('interchange')) \
                      .withColumn("sum_of_gross_sales", expr("sum_gross_credit_exchanged - sum_gross_debit_exchanged")) \
                      .withColumn("sum_of_transaction_costs", expr("commission + markup + scheme_fees + interchange")) \
                      .withColumn('month', lit(start))
                           
  # refund
  refund_cols = payments_info \
                      .filter(col('type') == 'Refunded') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('gross_debit_exchanged').alias('sum_of_refund_amount'), \
                           sum('commission').alias('commission'), \
                           sum('markup').alias('markup'), \
                           sum('scheme_fees').alias('scheme_fees'), \
                           sum('interchange').alias('interchange')) \
                      .withColumn("sum_of_refund_fee", expr("commission + markup + scheme_fees + interchange")) \
                      .withColumn('month', lit(start))
                           
  # refund reversed
  refund_reversed_cols = payments_info \
                      .filter(col('type') == 'RefundedReversed') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('gross_credit_exchanged').alias('sum_of_refund_reversed')) \
                      .withColumn('month', lit(start))
                           
  # fee
  fee_cols = payments_info \
                      .filter(col('type') == 'Fee') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('net_debit').alias('sum_of_fee')) \
                      .withColumn('month', lit(start))

  # chargeback
  chargeback_cols = payments_info \
                      .filter(col('type') == 'Chargeback') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('gross_debit_exchanged').alias('sum_of_chargeback'), \
                           sum('commission').alias('commission'), \
                           sum('markup').alias('markup'), \
                           sum('scheme_fees').alias('scheme_fees'), \
                           sum('interchange').alias('interchange')) \
                      .withColumn("sum_of_chargeback_fee", expr("commission + markup + scheme_fees + interchange")) \
                      .withColumn('month', lit(start))
                           
  # chargeback reversed
  chargeback_reversed_cols = payments_info \
                      .filter(col('type') == 'ChargebackReversed') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('net_credit').alias('sum_of_chargeback_reversed')) \
                      .withColumn('month', lit(start))
                           
   # invoice deduction
  invoice_deduction_cols = payments_info \
                      .filter(col('type') == 'InvoiceDeduction') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('net_debit').alias('sum_net_debit'), \
                           sum('net_credit').alias('sum_net_credit')) \
                      .withColumn("sum_of_invoice_deduction", expr("sum_net_credit - sum_net_debit")) \
                      .withColumn('month', lit(start))
                           
   # balance transfer
  balance_transfer = payments_info \
                      .filter(col('type') == 'Balancetransfer') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('net_debit').alias('sum_net_debit'), \
                           sum('net_credit').alias('sum_net_credit')) \
                      .withColumn("sum_of_balance_transfer", expr("sum_net_credit - sum_net_debit")) \
                      .withColumn('month', lit(start))
                           
   # deposit correction
  deposit_correction_cols = payments_info \
                      .filter(col('type') == 'DepositCorrection') \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('net_debit').alias('sum_of_deposit_correction')) \
                      .withColumn('month', lit(start))
                           
  revenue_per_type_dfs = total_cols \
                .join(settled_cols, ["channel", 'country', 'month'], 'fullouter') \
                .join(refund_cols, ["channel", 'country', 'month'], 'fullouter') \
                .join(refund_reversed_cols, ["channel", 'country', 'month'], 'fullouter') \
                .join(fee_cols, ["channel", 'country', 'month'], 'fullouter') \
                .join(chargeback_cols, ["channel", 'country', 'month'], 'fullouter') \
                .join(chargeback_reversed_cols, ["channel", 'country', 'month'], 'fullouter') \
                .join(invoice_deduction_cols, ["channel", 'country', 'month'], 'fullouter') \
                .join(balance_transfer, ["channel", 'country', 'month'], 'fullouter') \
                .join(deposit_correction_cols, ["channel", 'country', 'month'], 'fullouter') \
                .fillna(0) \
                .select('month', 'channel', 'country', 'sum_of_gross_sales', 'sum_of_refund_amount', 'sum_of_refund_reversed', 'sum_of_fee', 'sum_of_refund_fee', 'sum_of_transaction_costs',
                       'sum_of_chargeback_fee', 'sum_of_chargeback', 'sum_of_chargeback_reversed', 'sum_of_invoice_deduction', 'sum_of_balance_transfer', 'sum_of_deposit_correction', 
                       'sum_of_net_payout')
  
  subscriptions.unpersist()
  payments_info.unpersist()
  total_cols.unpersist()
  settled_cols.unpersist()
  refund_cols.unpersist()
  refund_reversed_cols.unpersist()
  fee_cols.unpersist()
  chargeback_cols.unpersist()
  chargeback_reversed_cols.unpersist()
  invoice_deduction_cols.unpersist()
  balance_transfer.unpersist()
  deposit_correction_cols.unpersist()
  
  return revenue_per_type_dfs
                           
                           
                           
def create_month_report_between_dates(start, end):
  customers = customers_dfs \
              .filter(col('create_date').isNotNull() & (col('create_date') < end)) \
              .fillna("", ['channel', 'country'])
  
  subscriptions = subscriptions_dfs \
                  .filter(col('state') != 'pending') \
                  .filter(col('subscription_create_date') < end) \
                  .join(customers.select("customer_id", "customer_first_payment_date"), 'customer_id') \
                  .select('customer_id', "subscription_id", 'subscription_plan_id', 'subscription_create_date', 'subscription_start', 'subscription_end', 'channel', 'country', 'first_payment_date', 'customer_first_payment_date') \
                  .fillna("", ['channel', 'country'])
  
  get_channel_udf = udf(add_channel, StringType())     
  
  payments = payments_dfs \
            .filter(col('creation_date') < end) \
            .join(subscriptions.select("subscription_id", 'customer_id', 'subscription_end'), col('merchant_reference') == col('subscription_id'), 'leftouter') \
            .withColumn("channel", get_channel_udf("channel", "merchant_account")) \
            .fillna("", ['channel', 'country'])

  payments_info = payments_info_dfs \
                  .filter((col('creation_date') < end)) \
                  .join(subscriptions.select("subscription_id", "channel", 'country'), col('merchant_reference') == col('subscription_id'), 'leftouter') \
                  .withColumn("channel", get_channel_udf("channel", "merchant_account")) \
                  .fillna("", ['channel', 'country'])
  
  payments_info_rev = revenue_per_type_between_dates(start, end)  
  
  subscription_with_flags = subscriptions \
                        .withColumn('is_new_subscription', when(col('subscription_create_date') >= start, lit(1)).otherwise(lit(0))) \
                        .withColumn('is_active_subscription', when((col('subscription_end') > start), lit(1)).otherwise(lit(0))) \
                        .withColumn('is_active_subscriptions_next_month', when((col('subscription_end') > end), lit(1)).otherwise(lit(0))) \
                        .withColumn('is_paid_subscription', when((col('subscription_end') > start) & (col('first_payment_date') < end), lit(1)).otherwise(lit(0))) \
                        .withColumn('is_paid_subscriptions_next_month', when((col('subscription_end') > end) & (col('first_payment_date') < end), lit(1)).otherwise(lit(0))) \
                        .withColumn('is_trialist_sub', when((col('is_new_subscription') > 0) & ((col('first_payment_date').isNull()) | (col('first_payment_date') >= start)), lit(1)).otherwise(lit(0))) \
                        .withColumn('is_reconnect_sub', when((col('is_new_subscription') > 0) & (col('first_payment_date').isNotNull()) & (col('first_payment_date') < start), lit(1)).otherwise(lit(0)))
  
  subscription_aggregated_per_customer = subscription_with_flags\
                        .groupBy('customer_id').agg(sum('is_new_subscription').alias('nr_new_subscription'), \
                                                    count('subscription_id').alias('subscription_count'), \
                                                    sum('is_trialist_sub').alias('nr_trialist_sub'), \
                                                    sum('is_reconnect_sub').alias('nr_reconnect_sub'), \
                                                    max('subscription_end').alias('customer_max_subscription_end'), \
                                                    sum('is_active_subscription').alias('nr_active_subscriptions'), \
                                                    sum('is_active_subscriptions_next_month').alias('nr_active_subscriptions_next_month'), \
                                                    sum('is_paid_subscription').alias('nr_paid_subscriptions'), \
                                                    sum('is_paid_subscriptions_next_month').alias('nr_paid_subscriptions_next_month'), \
                                                    min('customer_first_payment_date').alias('customer_first_payment_date'))
  
  subscription_cols_per_customer = subscription_aggregated_per_customer \
                        .withColumn('is_active', when(col('customer_max_subscription_end') > start, lit(1)).otherwise(lit(0))) \
                        .withColumn('is_trialist', when((col('nr_new_subscription') > 0) & ((col('customer_first_payment_date').isNull()) | (col('customer_first_payment_date') >= start)), lit(1)).otherwise(lit(0))) \
                        .withColumn('is_reconnect', when((col('nr_new_subscription') > 0) & (col('customer_first_payment_date').isNotNull()) & (col('customer_first_payment_date') < start), lit(1)).otherwise(lit(0))) \
                        .withColumn("unattributed_new_subs", expr("nr_trialist_sub + nr_reconnect_sub - is_trialist - is_reconnect")) \
                        .select('customer_id', 'is_active', 'is_trialist', 'is_reconnect', 'unattributed_new_subs', 'subscription_count', 'nr_active_subscriptions', 'nr_active_subscriptions_next_month', 'nr_paid_subscriptions', 'nr_paid_subscriptions_next_month')
  
  payments_cols = payments  \
                  .filter(col('creation_date') >= start) \
                  .withColumn('is_payment', when(col("net_credit") > 1, lit(True)).otherwise(lit(False))) \
                  .groupBy('channel', 'country').agg(sum('net_credit').alias('net_credit'), \
                                                       sum(col('has_chargeback').cast("long")).alias('has_chargeback'), \
                                                       sum(col('has_reversed_chargeback').cast("long")).alias('has_reversed_chargeback'), \
                                                       sum(col('has_refund').cast("long")).alias('has_refund'),
                                                       sum(col('has_reversed_refund').cast("long")).alias('has_reversed_refund'),
                                                       sum(col('is_payment').cast("long")).alias('nr_payments')) \
                  .withColumn('month', lit(start))
  
  # We don't need this one, but weirdly enough does joining this one later on prevent an assertionerror
  payments_cols_customers = payments  \
                          .filter(col('creation_date') >= start) \
                          .select('customer_id') \
                          .dropDuplicates()

  payments_total_cols = payments  \
                        .groupBy('customer_id').agg(sum('net_received').alias('net_received_total'), \
                                                    max('subscription_end').alias('customer_max_end_date')) \
                        .withColumn('is_paying', when(col('net_received_total') > 1, lit(1)).otherwise(lit(0))) \
                        .select('customer_id', 'is_paying')

  payments_info_cols = payments_info.select('channel', 'country', 'net_debit', 'net_credit', 'commission', 'markup', 'scheme_fees', 'interchange', 'net_currency') \
                      .filter(col('creation_date') >= start) \
                      .fillna(0) \
                      .groupBy(['channel', 'country']) \
                      .agg(sum('net_debit').alias('net_debit'), \
                           sum('net_credit').alias('net_credit'), \
                           sum('commission').alias('commission'), \
                           sum('markup').alias('markup'), \
                           sum('scheme_fees').alias('scheme_fees'), \
                           sum('interchange').alias('interchange'), \
                           max('net_currency').alias('net_currency')) \
                      .withColumn("net_received", expr("net_credit - net_debit")) \
                      .withColumn("total_fees", expr("commission + markup + scheme_fees + interchange")) \
                      .withColumn('month', lit(start))
  
  # Note that this is an inner join, we are only interested in customer records with any subscription
  month_report_1 = customers.select('customer_id', 'channel', 'country').withColumn('month', lit(start)) \
                  .join(subscription_cols_per_customer, ['customer_id']) \
                  .groupBy("month", 'channel', 'country') \
                  .agg(sum('is_trialist').alias('nr_trials'), \
                       sum('is_reconnect').alias('nr_reconnects'), \
                       sum('unattributed_new_subs').alias('nr_unattributed_new_subs'), \
                       sum('is_active').alias('nr_active_customers'), \
                       sum('nr_active_subscriptions').alias('nr_active_subscriptions'), \
                       sum('nr_active_subscriptions_next_month').alias('nr_active_subscriptions_next_month'), \
                       sum('nr_paid_subscriptions').alias('nr_paid_subscriptions'), \
                       sum('nr_paid_subscriptions_next_month').alias('nr_paid_subscriptions_next_month')) \
                  .withColumn("active_subscription_churn_rate", expr("1 - (nr_active_subscriptions_next_month / nr_active_subscriptions)")) \
                  .withColumn("paid_subscription_churn_rate", expr("1 - (nr_paid_subscriptions_next_month  / nr_paid_subscriptions)")) \
                  .fillna(0).fillna("").fillna(False)  
  
  month_report_2 = customers.select('customer_id', 'channel', 'country').withColumn('month', lit(start)) \
                  .join(payments_total_cols.select("customer_id", "is_paying"), ['customer_id'], 'leftouter') \
                  .join(payments_cols_customers.select('customer_id'), ['customer_id'], 'leftouter') \
                  .join(subscription_cols_per_customer.select("customer_id", 'is_active', 'subscription_count'), ['customer_id']) \
                  .filter((col('is_active') == 1) & (col('is_paying') == 1)) \
                  .select("month", 'channel', 'country', 'is_paying', 'subscription_count') \
                  .groupBy("month", 'channel', 'country') \
                  .agg(sum('is_paying').alias('nr_paid_customers'), \
                       sum('subscription_count').alias('nr_paid_customers_subscriptions')) \
                  .fillna(0).fillna("").fillna(False)
  
  month_report_3 = payments_info_cols \
                  .groupBy('month', 'channel', 'country') \
                  .agg(max('net_currency').alias('net_currency'), \
                       sum('net_debit').alias('net_debit'), \
                       sum('net_credit').alias('net_credit'), \
                       sum('net_received').alias('net_received'), \
                       sum('commission').alias('commission'), \
                       sum('markup').alias('markup'), \
                       sum('scheme_fees').alias('scheme_fees'), \
                       sum('interchange').alias('interchange'), \
                       sum('total_fees').alias('total_fees')) \
                  .select("month", "channel", 'country', 'net_currency', 'net_debit', 'net_credit', 'net_received', 'commission', 'markup', 'scheme_fees', 'interchange', 'total_fees')

  month_report_4 = payments_cols \
                  .select('month', 'channel', 'country', 'nr_payments', 'has_chargeback', 'has_reversed_chargeback', 'has_refund', 'has_reversed_refund') \
                  .groupBy('month', 'channel', 'country') \
                  .agg(sum('nr_payments').alias('nr_payments'), \
                       sum('has_chargeback').alias('nr_chargebacks'), \
                       sum('has_reversed_chargeback').alias('nr_reversed_chargebacks'), \
                       sum('has_refund').alias('nr_refunds'), \
                       sum('has_reversed_refund').alias('nr_reversed_refunds'), \
                  )

  month_report = month_report_1 \
                .join(month_report_2, ["channel", 'country', 'month'], 'fullouter') \
                .join(month_report_3, ["channel", 'country', 'month'], 'fullouter') \
                .join(month_report_4, ["channel", 'country', 'month'], 'fullouter') \
                .join(payments_info_rev, ["channel", 'country', 'month'], 'fullouter')

  month_report = month_report \
                .select('month','channel','country','nr_trials','nr_reconnects','nr_unattributed_new_subs','nr_paid_customers','nr_paid_customers_subscriptions',\
                                     'nr_active_customers', 'nr_active_subscriptions', 'nr_active_subscriptions_next_month', 'nr_payments','nr_chargebacks',\
                                     'nr_paid_subscriptions', 'nr_paid_subscriptions_next_month', 'active_subscription_churn_rate', 'paid_subscription_churn_rate',\
                                     'nr_reversed_chargebacks','nr_refunds', 'nr_reversed_refunds', 'net_currency', \
                                     'net_debit', 'net_credit', 'net_received', 'commission', 'markup', 'scheme_fees', 'interchange', 'total_fees', \
                                     'sum_of_gross_sales', 'sum_of_refund_amount', 'sum_of_refund_reversed', 'sum_of_fee', 'sum_of_refund_fee', 'sum_of_transaction_costs', \
                                     'sum_of_chargeback_fee', 'sum_of_chargeback', 'sum_of_chargeback_reversed', 'sum_of_invoice_deduction', 'sum_of_balance_transfer', \
                                     'sum_of_deposit_correction', 'sum_of_net_payout').fillna(0)
  month_report_1.unpersist()
  month_report_2.unpersist()
  month_report_3.unpersist()
  month_report_4.unpersist()
  month_report.unpersist()
  payments_info_rev.unpersist()
  payments_cols_customers.unpersist()
  payments_info_cols.unpersist()
  payments_info.unpersist()
  customers.unpersist()
  subscriptions.unpersist()
  subscription_with_flags.unpersist()
  subscription_aggregated_per_customer.unpersist()
  subscription_cols_per_customer.unpersist()
  payments.unpersist()
  payments_cols.unpersist()
  payments_total_cols.unpersist()
  
  return month_report

# COMMAND ----------

now = datetime.now()

dateEnd = '%s-%s-01' % (now.year, now.month)
dateRange = pd.date_range(start = START, end = dateEnd, freq ='MS')

insert_passed_list = []
for start in dateRange:
  timer = time.time()
  
  end = start + pd.DateOffset(months=1)
  print("Start {} to {}".format(start, end))
  
  month_report = create_month_report_between_dates(to_timestamp(lit(start)), to_timestamp(lit(end)))
  insert_passed = ctx['MonthReportService'].run_insert(month_report)
  insert_passed_list.append(insert_passed)
  
  print("Duration %.2f s (found records: %s)" % (time.time() - timer, month_report.count()))
  
customers_dfs.unpersist()
subscriptions_dfs.unpersist()
payments_dfs.unpersist()
payments_info_dfs.unpersist()

# COMMAND ----------

if False in insert_passed_list:
  sys.exit("Error, Month Report data is not inserted!")
