# Databricks notebook source
# Python standard library
import sys

# Python open source libraries
import pandas as pd
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import min, lit, first, udf

# Python DutchChannels libraries
from dc_azure_toolkit import sql
from data_sql.main import main as data_sql_main

# COMMAND ----------

DEBUG = False # Whether or not to enable debug mode. Selects a small sample of the data to use as test case.

ENVIRONMENT = 'prd'

MERCHANT_ACCOUNT_MAPPER = {
  "WithLove": 'lov',
  "DutchChannelsBVNL": 'nfn'
}

# COMMAND ----------

ctx = data_sql_main(environment=ENVIRONMENT, dbutils=dbutils)

authentication_dict = ctx['SQLAuthenticator'].get_authentication_dict()

payments_info_dfs = sql.select_table("payments_info", authentication_dict)
disputes_info_dfs = sql.select_table("disputes_info", authentication_dict)
subscription_dfs = sql.select_table("subscriptions", authentication_dict)

print("Length of payments_info before dropping duplicates: {}".format(payments_info_dfs.count())) 
payments_info_dfs = payments_info_dfs.dropDuplicates()
print("Length of payments_info after dropping duplicates: {}".format(payments_info_dfs.count())) 

if DEBUG:
  id_list = ['1316111908648994','1316262244535211','6229270885816779','6519158564350104', '1325909732462949', '1326192260456240', '6239204356463322', '6249313220612739'
            ,'1646306308318849', '4615997860450581']
  #id_list = ['1325909732462949']
  #id_list = payments_info_dfs.toPandas()['psp_reference'].unique().tolist()[0:1000]
  payments_info_dfs = payments_info_dfs.filter(payments_info_dfs['psp_reference'].isin(id_list))
  disputes_info_dfs = disputes_info_dfs.filter(disputes_info_dfs['psp_reference'].isin(id_list)) 
  subscription_dfs = subscription_dfs.filter(subscription_dfs['subscription_id'].isin(id_list)) 

# COMMAND ----------

def map_channel(merchant_account):
  try:
    return MERCHANT_ACCOUNT_MAPPER[merchant_account]
  except Exception:
    return ''

def add_channel(x1, x2):
  """
  Written to extend the channel for payments without a subscription_id (InvoiceDeduction, BalanceTransfer, etc).
  The idea is to get the channel based on the corresponding merchant_ref. If not possible, take the channel based on 
  the merchant_account.
  
  :param x1: channel
  :param x2: merchant_account
  
  :type x1: str
  :type x2: str
  """
  
  if isinstance(x1, type(None)) or x1 == '':
    return map_channel(x2)
  else: 
    return x1

# COMMAND ----------

print("Length of disputes_info before filtering: {}".format(disputes_info_dfs.count()))                                 
disputes_info_dfs = disputes_info_dfs.filter(disputes_info_dfs['record_type'].isin(['Chargeback']))
disputes_info_dfs = disputes_info_dfs.select(['psp_reference', 'cb_reason_code', 'is_voluntary_chargeback', 'is_involuntary_chargeback'])
disputes_info_dfs = disputes_info_dfs[['psp_reference', 'cb_reason_code', 'is_voluntary_chargeback', 'is_involuntary_chargeback']]
disputes_info_dfs = disputes_info_dfs.dropDuplicates()      
print("Length of disputes_info after filtering: {}".format(disputes_info_dfs.count())) 

payments_info_dfs = payments_info_dfs.na.fill(0, ['commission', 'markup', 'scheme_fees','interchange', 'gross_debit', 'gross_credit', 'net_debit', 'net_credit'])
payments_info_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['Chargeback', 'Settled', 'Refunded', 'ChargebackReversed', 'RefundedReversed']))
payments_info_cb_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['Chargeback']))
payments_info_cb_rev_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['ChargebackReversed']))
payments_info_settled_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['Settled']))
payments_info_refunded_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['Refunded']))
payments_info_refunded_rev_dfs = payments_info_dfs.filter(payments_info_dfs['type'].isin(['RefundedReversed']))

payments_info_cb_dfs = payments_info_cb_dfs.join(disputes_info_dfs, 
                                                 payments_info_cb_dfs.psp_reference == disputes_info_dfs.psp_reference, 
                                                 "left") \
                      .drop(disputes_info_dfs.psp_reference)

payments_dfs = payments_info_dfs.select(['psp_reference', 'merchant_reference', 'merchant_account']).dropDuplicates()

# Add creation_date column
temp_dfs = payments_info_settled_dfs.select(['psp_reference', 'creation_date']) \
           .groupby(['psp_reference']) \
           .agg(min('creation_date')) \
           .withColumnRenamed('min(creation_date)', 'creation_date') \

payments_dfs = payments_dfs.join(temp_dfs, \
                                 ['psp_reference'], \
                                 "left") \

# Add channel and country
Channel = udf(add_channel, StringType())  
subscriptions_dfs = sql.select_table("subscriptions", authentication_dict).select(['subscription_id', "channel", "country"])

payments_dfs = payments_dfs.join(subscriptions_dfs, \
                                 payments_dfs.merchant_reference == subscriptions_dfs.subscription_id, \
                                 'left') \
                          .withColumn("channel", Channel("channel", "merchant_account")) \
                          .fillna("", ['channel', 'country'])

# Add gross_currency
temp_dfs = payments_info_dfs.select(['psp_reference', 'gross_currency']).dropDuplicates()
payments_dfs = payments_dfs.join(temp_dfs, \
                                 ['psp_reference'], \
                                 "left") \

# Add gross_debit and gross_credit
temp_dfs = payments_info_dfs.select(['psp_reference', 'gross_debit', 'gross_credit']) \
          .groupby(['psp_reference']) \
          .sum() \
          .withColumnRenamed('sum(gross_debit)', 'gross_debit') \
          .withColumnRenamed('sum(gross_credit)', 'gross_credit')
payments_dfs = payments_dfs.join(temp_dfs, \
                                 ['psp_reference'], \
                                 "left") \

# Add gross_received
payments_dfs = payments_dfs.withColumn('gross_received', (payments_dfs['gross_credit'] - payments_dfs['gross_debit']))   

# Add net_currency
temp_dfs = payments_info_dfs.select(['psp_reference', 'net_currency']).dropDuplicates()
payments_dfs = payments_dfs.join(temp_dfs, 
                                 ['psp_reference'], \
                                 "left") \

# Add net_debit and net_credit
temp_dfs = payments_info_dfs.select(['psp_reference', 'net_debit', 'net_credit']) \
          .groupby(['psp_reference']) \
          .sum() \
          .withColumnRenamed('sum(net_debit)', 'net_debit') \
          .withColumnRenamed('sum(net_credit)', 'net_credit')
payments_dfs = payments_dfs.join(temp_dfs, 
                                 ['psp_reference'], \
                                 "left") \

# Add gross_received
payments_dfs = payments_dfs.withColumn('net_received', (payments_dfs['net_credit'] - payments_dfs['net_debit']))   

# Add commission 
temp_dfs = payments_info_dfs.select(['psp_reference', 'commission']) \
          .groupby(['psp_reference']) \
          .sum() \
          .withColumnRenamed("sum(commission)", "commission")
payments_dfs = payments_dfs.join(temp_dfs, 
                                 ['psp_reference'], \
                                 "left") \

# Add markup
temp_dfs = payments_info_dfs.select(['psp_reference', 'markup']) \
          .groupby(['psp_reference']) \
          .sum() \
          .withColumnRenamed("sum(markup)", "markup")
payments_dfs = payments_dfs.join(temp_dfs, \
                                 ['psp_reference'], \
                                 "left") \

# Add scheme_fees
temp_dfs = payments_info_dfs.select(['psp_reference', 'scheme_fees']) \
          .groupby(['psp_reference']) \
          .sum() \
          .withColumnRenamed("sum(scheme_fees)", "scheme_fees")
payments_dfs = payments_dfs.join(temp_dfs, \
                                 ['psp_reference'], \
                                 "left") \

# Add interchange
temp_dfs = payments_info_dfs.select(['psp_reference', 'interchange']) \
          .groupby(['psp_reference']) \
          .sum() \
          .withColumnRenamed("sum(interchange)", "interchange")
payments_dfs = payments_dfs.join(temp_dfs, \
                                 ['psp_reference'], \
                                 "left") \

# Add total_fees  
sum_columns = (payments_dfs.commission + payments_dfs.markup + payments_dfs.scheme_fees + payments_dfs.interchange)
payments_dfs = payments_dfs.withColumn('total_fees' \
                                       , sum_columns)

# Add exchange_rate, we should take a weighted average, but has turned out to be rather annoying -> do later
payments_dfs = payments_dfs.withColumn("exchange_rate", lit(None).cast(StringType()))

# Add suspect_fraud
payments_dfs = payments_dfs.withColumn("suspect_fraud", lit(None).cast(StringType()))

# Add has_chargeback
payments_info_cb_dfs = payments_info_cb_dfs.withColumn("has_chargeback", lit(1))
payments_dfs = payments_dfs.join(payments_info_cb_dfs.select(['psp_reference','has_chargeback']), \
                                ['psp_reference'], \
                                "left") \

# Add has_reversed_chargeback
payments_info_cb_rev_dfs = payments_info_cb_rev_dfs.withColumn("has_reversed_chargeback", lit(1))
payments_dfs = payments_dfs.join(payments_info_cb_rev_dfs.select(['psp_reference','has_reversed_chargeback']), \
                                 ['psp_reference'], \
                                "left") \

# Add has_refund
payments_info_refunded_dfs = payments_info_refunded_dfs.withColumn("has_refund", lit(1))
payments_dfs = payments_dfs.join(payments_info_refunded_dfs.select(['psp_reference','has_refund']), \
                                 ['psp_reference'], \
                                "left") \

# Add has_reverse_refund
payments_info_refunded_rev_dfs = payments_info_refunded_rev_dfs.withColumn("has_reversed_refund", lit(1))
payments_dfs = payments_dfs.join(payments_info_refunded_rev_dfs.select(['psp_reference','has_reversed_refund']), \
                                 ['psp_reference'], \
                                "left") \

# Add is_voluntary and is_not_voluntary
temp_dfs = payments_info_cb_dfs.select(['psp_reference','is_voluntary_chargeback', 'is_involuntary_chargeback'])
payments_dfs = payments_dfs.join(temp_dfs, \
                                 ['psp_reference'], \
                                 "left") \
                           .withColumnRenamed('is_voluntary_chargeback', 'cb_is_voluntary') \
                           .withColumnRenamed('is_involuntary_chargeback', 'cb_not_voluntary')

# Impute null values with 0
payments_dfs = payments_dfs.na.fill(0, ['has_refund', 'has_chargeback', 'has_reversed_chargeback'])

#Reorder columns in desired order
payments_dfs = payments_dfs.select(['psp_reference', 'merchant_reference', 'merchant_account', 'channel', 
                                    'country', 'creation_date', 'gross_currency', 'gross_debit',
                                    'gross_credit', 'gross_received', 'net_currency', 
                                    'net_debit', 'net_credit', 'net_received',
                                    'exchange_rate', 'commission', 'markup',
                                    'scheme_fees', 'interchange', 'total_fees','suspect_fraud', 
                                    'has_chargeback', 'has_reversed_chargeback', 'has_refund', 
                                    'has_reversed_refund', 'cb_is_voluntary','cb_not_voluntary'])


# COMMAND ----------

payments_service = ctx['PaymentsService']
insert_passed = payments_service.run_insert(payments_dfs)

if insert_passed == False:
  sys.exit("Error, payments data is not inserted!")
