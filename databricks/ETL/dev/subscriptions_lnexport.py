# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import udf, array, lit, col

import sys

import numpy as np
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import math

# COMMAND ----------

def get_data_explorer_authentication_dict(resource_name):
  """
  Gets all parameters needed to authenticate with known data explorer resources. Can be used to retrieve data from data explorer.
  
  :param resource_name: name of the data explorer resource, for example uelprdadx
  :type resource_name: str
  """
  supported_resources = ['uelprdadx', 'ueldevadx']
  
  if resource_name not in supported_resources:
    dbutils.notebook.exit("Enter a valid data explorer resource group!")
    
  if resource_name == 'uelprdadx':
    cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
    client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" 
    client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" 
    authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID
    authentication_dict = {
      'cluster': cluster,
      'client_id': client_id,
      'client_secret': client_secret,
      'authority_id':authority_id
    }
    
  elif resource_name == 'ueldevadx':
    cluster = "https://ueldevadx.westeurope.kusto.windows.net" # id 
    client_id = "8637a5e9-864d-4bd9-bb2e-be8ce991dbf6" # vervangen
    client_secret = "OJ.vcZo42y8_Kr5tsAXc6ImK~5b~s9k~0n" # 
    authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID (Vervangen)
    authentication_dict = {
      'cluster': cluster,
      'client_id': client_id,
      'client_secret': client_secret,
      'authority_id':authority_id
    }
  return authentication_dict

# COMMAND ----------

#https://docs.microsoft.com/en-us/azure/data-explorer/connect-from-databricks
#https://github.com/Azure/azure-kusto-spark/blob/master/samples/src/main/python/pyKusto.py

query = """
subscriptions
| project Date, SubscriptionId = ['Subscription.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionStartDate = ['Subscription.SubscriptionStart'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], CustomerRegistrationCountry = ['Customer.RegistrationCountry'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id'], SubscriptionPlanBillingFrequency = ['SubscriptionPlan.BillingFrequency'], AdyenPayments = ['Adyen.Payment.Payments'], AdyenDisputes = ['Adyen.Dispute.Disputes'], RecurringEnabled = ['Subscription.RecurringEnabled'], AdyenFirstPaymentReceived = ['Adyen.Payment.FirstPaymentReceived'], AdyenTotalAmountInSettlementCurrency = ['Adyen.Payment.TotalAmountInSettlementCurrency'], SubscriptionPrice = ['SubscriptionPlan.Price'], SubscriptionCurrency = ['SubscriptionPlan.Currency'], SubscriptionState = ['Subscription.State'] 
| where Date >= startofday(datetime("2021-1-4")) and Date < startofday(datetime("2021-1-4") + 1d)
"""

def run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'uel', df_format = 'dfs'):
  """
  Authenticates with a given data explorer cluster, and gets data executing a query.
  
  :param query:
  :param resource_name:
  :param kustoDatabase:
  :param df_format:
  
  :type query:
  :type resource_name:
  :type kustoDatabase:
  :type df_format:
  """
  
  supported_df_format = ['dfs', 'df']
  if df_format not in supported_df_format:
    dbutils.notebook.exit("Dataframe format not supported!")
    
    
  authentication_dict = get_data_explorer_authentication_dict(resource_name)
  
  pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
  kustoOptions = {"kustoCluster":authentication_dict['cluster'] 
                   ,"kustoDatabase":kustoDatabase
                  ,"kustoAadAppId":authentication_dict['client_id'] 
                  ,"kustoAadAppSecret":authentication_dict['client_secret']
                  , "kustoAadAuthorityID":authentication_dict['authority_id']}

  df  = pyKusto.read. \
              format("com.microsoft.kusto.spark.datasource"). \
              option("kustoCluster", kustoOptions["kustoCluster"]). \
              option("kustoDatabase", kustoOptions["kustoDatabase"]). \
              option("kustoQuery", query). \
              option("kustoAadAppId", kustoOptions["kustoAadAppId"]). \
              option("kustoAadAppSecret", kustoOptions["kustoAadAppSecret"]). \
              option("kustoAadAuthorityID", kustoOptions["kustoAadAuthorityID"]). \
              load()
  
  if df_format == 'df':
    df = df.toPandas()
  
  return df

# COMMAND ----------

subscription_plan_id_lookup_query = """
SubscriptionPlan
| project subscription_id = SubscriptionPlanId,channel_country = strcat(Channel, " ", Country), channel = Channel, country = Country, contract_duration = ContractDuration
| join kind=leftouter (
        Country
        | project CountryId, Market
        ) on $left.country == $right.CountryId
| project subscription_id, channel_country, channel, country, contract_duration, Market
"""

dfs_subscription_plan_id = run_data_explorer_query(subscription_plan_id_lookup_query, resource_name ='uelprdadx', kustoDatabase = 'shared', df_format = 'dfs')

query = """
BillingFrequency
| project billing_frequency = BillingFrequencyId, type = Type
"""

dfs_billing_frequencies = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'shared', df_format = 'dfs')

del query

query = """
SELECT t1.CCY, t1.RATE 
FROM static_tables.forex_rates AS t1;
"""
dfs_forex_rates = spark.sql(query)

del query

query = """
SELECT t1.country, t1.VAT 
FROM static_tables.country_vat AS t1;
"""
dfs_vat = spark.sql(query)
dfs_subscription_plan_id = dfs_subscription_plan_id.join(dfs_vat.select(['country','VAT']),dfs_subscription_plan_id['country'] == dfs_vat['country'],'left')

del query, dfs_vat

# COMMAND ----------

query = """
subscriptions_lnexport
| where Date >= startofday(now()) and Date < startofday(now() + 1d) 
| summarize count = count()
"""
nr_records_today = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'dfs').select('count').first()[0]
if nr_records_today == 0:
  write = True
else:
  write = False
  dbutils.notebook.exit("TERMINATE - table already contains data of today.")


# COMMAND ----------

query = """
subscriptions
| where Date >= startofday(now()) and Date < startofday(now() + 1d) 
| summarize count = count()
"""

nr_records_today_subscriptions = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'dfs').select('count').first()[0]
if nr_records_today_subscriptions > 0:
  write = True
else:
  write = False
  dbutils.notebook.exit("TERMINATE - subscription table of today does not contain any data.")
  

# COMMAND ----------

query = """
subscriptions_rawdatamod
| where Date >= startofday(now()) and Date < startofday(now() + 1d) 
| summarize count = count()
"""

nr_records_today_rawdatamod = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'dfs').select('count').first()[0]
if nr_records_today_rawdatamod > 0:
  write = True
else:
  write = False
  dbutils.notebook.exit("TERMINATE - subscriptions_rawdatamod table of today does not contain any data.")

if nr_records_today_rawdatamod != nr_records_today_subscriptions:
  write = False
  dbutils.notebook.exit("TERMINATE - something went wrong in the rawdatamod calculations, unequal number of rows compared to subscriptions.")

# COMMAND ----------

query = """
subscriptions
| where Date >= startofday(datetime("2021-1-4")) and Date < startofday(datetime("2021-1-4") + 1d)
| join (
        subscriptions_rawdatamod
        | where Date >= startofday(datetime("2021-1-4")) and Date < startofday(datetime("2021-1-4") + 1d)
        | project Date, ['Subscription.Id'] ,DCBank, TrialOnlyUser, TrialChargeback, Country,Market, TrialChurn, HistoricSubscriptionStartDate, Type
        ) on $left.['Subscription.Id'] == $right.['Subscription.Id'] 
| project Date, SubscriptionId = ['Subscription.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionStartDate = ['Subscription.SubscriptionStart'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], CustomerRegistrationCountry = ['Customer.RegistrationCountry'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id'], SubscriptionPlanBillingFrequency = ['SubscriptionPlan.BillingFrequency'], AdyenPayments = ['Adyen.Payment.Payments'], AdyenDisputes = ['Adyen.Dispute.Disputes'], RecurringEnabled = ['Subscription.RecurringEnabled'], AdyenFirstPaymentReceived = ['Adyen.Payment.FirstPaymentReceived'], AdyenTotalAmountInSettlementCurrency = ['Adyen.Payment.TotalAmountInSettlementCurrency'], SubscriptionPrice = ['SubscriptionPlan.Price'], SubscriptionCurrency = ['SubscriptionPlan.Currency'], SubscriptionState = ['Subscription.State'], AdyenPaymentFirstPayment = ["Adyen.Payment.FirstPayment"], DCBank = DCBank, TrialOnlyUser= TrialOnlyUser,TrialChargeback = TrialChargeback, Country= Country, Market = Market, TrialChurn = TrialChurn,HistoricSubscriptionStartDate = HistoricSubscriptionStartDate, Type = Type
"""

dfs = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'dfs')
dfs = dfs.join(dfs_subscription_plan_id.select(['subscription_id','VAT']),dfs['SubscriptionPlanId'] == dfs_subscription_plan_id['subscription_id'],'left')
dfs.show()
del query

# COMMAND ----------

def add_column_concctry(x1, x2):
  """
  Concatenating channel and country together. An example is nfnNL.
  
  :param x1: Country based on the subscription_plan_id of the customer
  :param x2: Channel (ID)
  :type x1: str
  :type x2: str
  
  Question:
  - Shouldnt we just get the concat from subscription_plan_id. It's more reliable.
    -Pay attention to: vg becomes tt
  """
  if x1 == 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d':
    return 'nfn'+x2
  elif x1 == '25b748df-e99f-5ec9-aea5-967a73c88381':
    return 'vg'+x2
  elif x1 == 'a495e56a-0139-58f3-9771-cec7690df76e':
    return 'lov'+x2
  return "NOT FOUND"

Concctry = udf(add_column_concctry, StringType())
dfs = dfs.withColumn('Concctry',Concctry('Channel','Country'))

# COMMAND ----------

def add_column_conc_region(x1, x2):
  """
  Concatenating channel and region together. An example is nfnNordics.
  
  :param x1: Country based on the subscription_plan_id of the customer
  :param x2: The market this subscription is from
  :type x1: str
  :type x2: str
  
  """
  if x1 == 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d':
    return 'nfn'+x2
  elif x1 == '25b748df-e99f-5ec9-aea5-967a73c88381':
    return 'vg'+x2
  elif x1 == 'a495e56a-0139-58f3-9771-cec7690df76e':
    return 'lov'+x2
  return "NOT FOUND"

ConcRegion = udf(add_column_conc_region, StringType())
dfs = dfs.withColumn('ConcRegion',ConcRegion('Channel','Market'))

# COMMAND ----------

def add_column_create_month(x1):
  """
  Filter the month from the created date.
  
  :param x1: Subscription.CreatedDate from the subscription table in data explorer
  :type x1: datetime

  """
  if type(x1) == type(None):
    return 0
  return int(x1.month)

CreateMonth = udf(add_column_create_month, IntegerType())
dfs = dfs.withColumn('CreateMonth',CreateMonth('SubscriptionCreatedDate'))

# COMMAND ----------

def add_column_create_year(x1):
  """
  Filter the year from the created date.
  
  :param x1: Subscription.CreatedDate from the subscription table in data explorer
  :type x1: datetime

  """
  if type(x1) == type(None):
    return 2100
  return int(x1.year)

CreateYear = udf(add_column_create_year, IntegerType())
dfs = dfs.withColumn('CreateYear',CreateYear('SubscriptionCreatedDate'))
dfs.show(2)

# COMMAND ----------

def add_column_end_month(x1):
  """
  Filter the month from the end date.
  
  :param x1: subscription_end from the subscription table in data explorer
  :type x1: datetime

  """
  if type(x1) == type(None):
    return 0
  return int(x1.month)

EndMonth = udf(add_column_end_month, IntegerType())
dfs = dfs.withColumn('EndMonth',EndMonth('SubscriptionEndDate'))
dfs.show(2)

# COMMAND ----------

def add_column_end_year(x1):
  """
  The year of subscription_end from the subscription table in data explorer.
  
  :param x1: subscription_end from the subscription table in data explorer
  :type x1: timestamp

  """
  if type(x1) == type(None):
    return 2100
  return int(x1.year)

EndYear = udf(lambda x: add_column_end_year(x), IntegerType())
dfs = dfs.withColumn('EndYear', EndYear("SubscriptionEndDate"))


# COMMAND ----------

def add_column_paid_month(x1, x2, x3):
  """
  The month of the first payment of the subscription, if anything was paid at all.
  
  :param x1: payments from the subscription table in data explorer
  :param x2: disputes from the subscription table in data explorer
  :param x3: first_payment from the subscription table in data explorer
  :type x1: int
  :type x2: int
  :type x3: timestamp
  """
  if type(x1) == type(None):
    x1 = 0
  if type(x2) == type(None):
    x2 = 0
  if x1 - x2 > 0:
    return int(x3.month)
  return 0
PaidMonth = udf(add_column_paid_month, IntegerType())
dfs = dfs.withColumn('PaidMonth', PaidMonth("AdyenPayments","AdyenDisputes","AdyenPaymentFirstPayment"))
dfs.show(2)

# COMMAND ----------

def add_column_paid_year(x1, x2, x3):
  """
  The year of the first payment of the subscription, if anything was paid at all.
  
  :param x1: payments from the subscription table in data explorer
  :param x2: disputes from the subscription table in data explorer
  :param x3: first_payment from the subscription table in data explorer
  :type x1: int
  :type x2: int
  :type x3: timestamp
  """
  if type(x1) == type(None):
    x1 = 0
  if type(x2) == type(None):
    x2 = 0
  if x1 - x2 > 0:
    return int(x3.year)
  return 0
PaidYear = udf(add_column_paid_year, IntegerType())
dfs = dfs.withColumn('PaidYear', PaidYear("AdyenPayments","AdyenDisputes","AdyenPaymentFirstPayment"))
dfs.show(2)

# COMMAND ----------

def add_column_cohort_date(x1, x2, x3, x4):
  """
  Start of paid lifetime of customer. Created date date for trials. 
  
  :param x1: trial only user as calculated in rawdatamod
  :param x2: trial chargeback as calculated in rawdatamod
  :param x3: subscription created_date from table in data explorer
  :param x4: subscription end_date from table in data explorer
  :type x1: str
  :type x2: str
  :type x3: timestamp
  :type x4: timestamp
  """
  if (x1 == "Y" or x2 == "Y"):
    return x3.date()
  return x4.date()
CohortDate = udf(add_column_cohort_date, TimestampType())
dfs = dfs.withColumn('CohortDate', CohortDate("TrialOnlyUser","TrialChargeback","SubscriptionCreatedDate","SubscriptionEndDate"))
dfs.show(2)

# COMMAND ----------

def add_column_paid_start(x1):
  """
  The start of the paid lifetime.
  
  :param x1: historic subscription start date as calculated in rawdatamod
  :type x1: str
  """
  return x1
PaidStart = udf(add_column_paid_start, TimestampType())
dfs = dfs.withColumn('PaidStart', PaidStart("HistoricSubscriptionStartDate"))
dfs.show(2)

# COMMAND ----------

def add_column_paid_end(x1):
  """
  The end of the paid lifetime.
  
  :param x1: subscription end from the subscription table in data explorer
  :type x1: str
  """
  return x1
PaidEnd = udf(add_column_paid_end, TimestampType())
dfs = dfs.withColumn('PaidEnd', PaidEnd("SubscriptionEndDate"))
dfs.show(2)

# COMMAND ----------

# Try to mimic the datedif function in excel.
def datedif_month(start_date, end_date):
  diff = end_date.date() - start_date.date()
  return int((diff.days / 30)) + 1
def datedif_days(start_date, end_date):
  diff = end_date.date() - start_date.date()
  return diff.days


# COMMAND ----------

def add_column_paid_sub_month(x1, x2, x3, x4):
  """
  UNFINISHED
  The number of months the subscriber is considered paid, determined by the paid start date and paid end date.
  
  :param x1: payments from the subscription table in data explorer
  :param x2: disputes from the subscription table in data explorer
  :param x3: paid start as calculated in previous cells
  :param x4: paid end as calculated in previous cells
  
  :type x1: int
  :type x1: int
  :type x1: timestamp
  :type x1: timestamp
  
  Question: 
  - hard to replicate the datediff function from excel. Slightly different interpretation okay?
    - what to do
  """
  if type(x1) == type(None):
    x1 = 0
  if type(x2) == type(None):
    x2 = 0
  if type(x3) == type(None) or type(x4) == type(None):
    return 0
  if x1 - x2 > 0:
    return datedif_month(x3, x4)
  return 0
PaidSubMonth = udf(add_column_paid_sub_month, IntegerType())
dfs = dfs.withColumn('PaidSubMonth', PaidSubMonth("AdyenPayments","AdyenDisputes", "PaidStart", "PaidEnd"))
#dfs.select('PaidSubMonth').show(200)

# COMMAND ----------

def add_column_nr_payment_cycles(x1, x2, x3, x4):
  """
  UNFINISHED
  The estimated number of payment cycles in days. Dependent on the billing frequency.
  
  :param x1: trial churn as calculated in rawdata_mod
  :param x2: historic subscription start date as calculated in rawdata_mod
  :param x3: subscription end date from the subscription table in data explorer
  :param x4: SubscriptionPlanBillingFrequency as calculated in rawdata_mod
  
  :type x1: str
  :type x2: timestamp
  :type x3: timestamp
  :type x4: int
  
  Questions:
  -formula: (if paid customer): nr days from start / nr of days in contract cycle * nr of months in contract cycle 
    - Why times the number of months? This looks like the nr_payments_months, rather then cycles.
  - the weird billing_frequencies (1 and 5) not added to table
  - VLOOKUP based on Type seems unnecassary: just take SubscriptionPlanBillingFrequency.
  """
  
  if type(x2) == type(None):
    x1 = datetime.now().date() + timedelta(days = 1)
  if type(x4) == type(None):
    x4 = 30
    
  #If we see the weird exceptions of billing_cycle = 1 or 5, consider the actual cycle length as a month; 30 days.  
  if x4 == 5 or x4 == 1:
    x4 = 30
  diff = 0.0
  if x1 != "Paid":
    diff = 0.0
  else:
    diff = x3.date() + timedelta(days=1) - x2.date()
    diff = diff.days
    
  return diff / float(x4)

NrPaymentCycles = udf(add_column_nr_payment_cycles, FloatType())
dfs = dfs.withColumn('NrPaymentCycles', NrPaymentCycles("TrialChurn","HistoricSubscriptionStartDate", "SubscriptionEndDate", "SubscriptionPlanBillingFrequency"))



# COMMAND ----------

def add_column_payment_errors(x1, x2, x3):
  """
  UNFINISHED
  The estimated number of payment errors. Based on actual payments vs. expected payments based on billing frequency.
  
  :param x1: payments from the subscription table in data explorer
  :param x2: disputes from the subscription table in data explorer
  :param x3: number of payment cycles as calculated in the previous cells
  
  :type x1: int
  :type x2: int
  :type x3: float
  
  Questions:
  -It seems like the multiplication in add_column_nr_payment_cycles is unnecessary. As we multiply with the same term here. 
  """
  if type(x1) == type(None):
    x1 = 0
  if type(x2) == type(None):
    x2 = 0
  
  if x1 - x2 >0:
    return x1 - x3
  
  return 0

NrPaymentErrors = udf(add_column_payment_errors, FloatType())
dfs = dfs.withColumn('NrPaymentErrors', NrPaymentErrors("AdyenPayments","AdyenDisputes", "NrPaymentCycles"))


# COMMAND ----------

def add_column_trial_days(x1, x2):
  """
  Estimated trial days of subscription.
  
  :param x1: historic subscription start date as calculated in rawdatamod
  :param x2: created date from the subscription table in data explorer
  
  :type x1: timestamp
  :type x2: timestamp

  """
  diff = x1.date() - (x2.date() - timedelta(days= -1))
  return diff.days

TrialDays = udf(add_column_trial_days, IntegerType())
dfs = dfs.withColumn('TrialDays', TrialDays("HistoricSubscriptionStartDate", "SubscriptionCreatedDate"))
dfs.filter(dfs['SubscriptionId'] == 'fefdd9bc-9243-468d-b7fe-7ff2a1d9e30e').show(10)

# COMMAND ----------

def add_column_cb_end_date(x1, x2):
  """
  End date of the subscription who did a chargeback in it's lifetime.
  
  :param x1: disputes from the subscription table in data explorer
  :param x2: subscription end date from the subscription table in data explorer
  
  :type x1: int
  :type x2: timestamp

  """
  if type(x1) == type(None):
    x1 = 0
  if type(x2) == type(None):
    x2 = datetime(1900,0,0,0,0,0).date()
  if x1 >= 1:
    return x2 + timedelta(days = 1)
  
  return None

CBEndDate = udf(add_column_cb_end_date, TimestampType())
dfs = dfs.withColumn('CBEndDate', CBEndDate("AdyenDisputes", "SubscriptionEndDate"))

# COMMAND ----------

def add_column_paid_lt_in_days(x1, x2):
  """
  Paid lifetime in days
  
  :param x1: historic subscription start date as calculated in rawdatamod
  :param x2: subscription end date from the subscription table in data explorer
  
  :type x1: timestamp
  :type x2: timestamp

  """
  if type(x2) == type(None):
    x2 = datetime(1900,0,0,0,0,0).date()
  
  diff = datedif_days(x1, x2) + 1
  if diff >= 0:
    return diff
  return -1000

PaidLTInDays = udf(add_column_paid_lt_in_days, IntegerType())
dfs = dfs.withColumn('PaidLTInDays', PaidLTInDays("HistoricSubscriptionStartDate", "SubscriptionEndDate"))

# COMMAND ----------

def add_column_paid_member_lt(x1, x2, x3, x4):
  """
  UNFINISHED
  The number of months the subscriber is considered paid, determined by the paid start date and paid end date.
  
  :param x1: payments from the subscription table in data explorer
  :param x2: disputes from the subscription table in data explorer
  :param x3: paid start as calculated in previous cells
  :param x4: paid end as calculated in previous cells
  
  :type x1: int
  :type x1: int
  :type x1: timestamp
  :type x1: timestamp
  
  Question: 
  - hard to replicate the datediff function from excel. Slightly different interpretation okay?
    - what to do
  """
  if type(x1) == type(None):
    x1 = 0
  if type(x2) == type(None):
    x2 = 0
  if type(x3) == type(None) or type(x4) == type(None):
    return 0
  if x1 - x2 > 0:
    return datedif_month(x3, x4)
  return 0
PaidMemberLT = udf(add_column_paid_member_lt, IntegerType())
dfs = dfs.withColumn('PaidMemberLT', PaidMemberLT("AdyenPayments","AdyenDisputes", "PaidStart", "PaidEnd"))

# COMMAND ----------

def add_column_avg_per_month(x1, x2, x3):
  """
  UNFINISHED
  Average payment per month, taking into consideration the VAT.
  
  :param x1: total amount settlement currency from the subscriptions table in data explorer
  :param x2: VAT percentage from the dbfs table 'vat'
  :param x3: paid member LT as calculated in the previous cells
  
  :type x1: float
  :type x2: float
  :type x3: int
  
  
  Questions;
  - Why is the VAT of NL 1.
  - Why cant we take the VAT of each country rather then platform.
  """
  if type(x1) == type(None):
    x1 = 0.0
  if type(x2) == type(None):
    x2 = 0.0
  if type(x3) == type(None):
    x3 = 0

  try:
    return x1 / x2 / x3
  except ZeroDivisionError:
    return 0.0


AvgPerMonth = udf(add_column_avg_per_month, FloatType())
dfs = dfs.withColumn('AvgPerMonth', AvgPerMonth("AdyenTotalAmountInSettlementCurrency", "VAT", "PaidMemberLT"))


# COMMAND ----------

added_cols = ["Concctry", "ConcRegion", "CreateMonth", "CreateYear", "EndMonth", "EndYear","PaidMonth","PaidYear","CohortDate", "PaidSubMonth", "NrPaymentCycles", "NrPaymentErrors", "HistoricSubscriptionStartDate", "TrialDays","CBEndDate", "PaidLTInDays","PaidStart","PaidEnd", "PaidMemberLT", "AvgPerMonth"]
base_cols = ['Date', 'SubscriptionId']
dfs_final = dfs.select(base_cols + added_cols)
dfs_final = dfs_final.withColumn("Created", lit(str(datetime.now(pytz.timezone('Europe/Amsterdam')))))

missing_vat_countries = dfs_subscription_plan_id.filter(dfs_subscription_plan_id['VAT'].isNull()).count()
print("Nr of countries without VAT percentages found: {}".format(missing_vat_countries))

# COMMAND ----------

def write_to_data_explorer(dfs, table_name, resource_name ='uelprdadx', kustoDatabase = 'uel', write = True):
  """
  Write a spark dataframe to a selected data explorer table. View the microsoft documentation on: https://docs.microsoft.com/en-us/azure/data-explorer/spark-connector.
  
  :param dfs: spark dataframe
  :param table_name: name of the table of the target destination
  :param resource_name: name of the resource group of the data explorer destination table
  :param kustoDatabase: the database in which to query on the selected resource_group
  :param write: whether to actually proceed writing to the target destination
  
  :type dfs: spark dataframe
  :type table_name: str
  :type resource_name: str
  :type kustoDatabase: str
  :type write: boolean
  """
  authentication_dict = get_data_explorer_authentication_dict(resource_name)

  pyKusto = SparkSession.builder.appName("kustoPySpark").getOrCreate()
  kustoOptions = {"kustoCluster":authentication_dict['cluster'] 
                   ,"kustoDatabase":kustoDatabase
                  ,"kustoAadAppId":authentication_dict['client_id'] 
                  ,"kustoAadAppSecret":authentication_dict['client_secret']
                  , "kustoAadAuthorityID":authentication_dict['authority_id']}

  if write:
    dfs.write. \
      format("com.microsoft.kusto.spark.datasource"). \
      option("kustoCluster",kustoOptions["kustoCluster"]). \
      option("kustoDatabase",kustoOptions["kustoDatabase"]). \
      option("kustoTable", table_name). \
      option("kustoAadAppId",kustoOptions["kustoAadAppId"]). \
      option("kustoAadAppSecret",kustoOptions["kustoAadAppSecret"]). \
      option("kustoAadAuthorityID",kustoOptions["kustoAadAuthorityID"]). \
      option("tableCreateOptions","CreateIfNotExist"). \
      mode("append"). \
      save()


resource_name = 'uelprdadx'
kustoDatabase = 'playground'
write_to_data_explorer(dfs_final, 'subscriptions_lnexport', resource_name, kustoDatabase, write)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


