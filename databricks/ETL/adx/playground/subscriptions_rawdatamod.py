# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import udf, array, lit, col, when

import sys

import numpy as np
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import math

# Python DutchChannels libraries
from dc_azure_toolkit import adx

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

def get_azure_sql_authentication_dict(server, db):
  """
  Gets all parameters needed to authenticate with known data explorer resources. Can be used to retrieve data from data explorer.
  
  :param server: abbreviated name of the server, for example dcreporting
  :param db: the database on the specified server
  :type server: str
  :type db: str
  """
  supported_servers = ['data-prd-sql']
  
  if server not in supported_servers:
    sys.exit("ERROR - Enter a valid azure SQL server!")
    
  if server == 'data-prd-sql':
    password = dbutils.secrets.get(scope = "data-prd-keyvault", key = "data-prd-sql-datateam-password")
    full_server_name = 'data-prd-sql.database.windows.net'
    database = db
    username = 'datateam'
    authentication_dict = {
      'full_server_name': full_server_name,
      'password': password,
      'database': database,
      'username':username
    }
    
  return authentication_dict


# COMMAND ----------

def run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'uel', df_format = 'dfs'):
  """
  Authenticates with a given data explorer cluster, and gets data executing a query.
  
  :param query: adx query to run on the target database
  :param resource_name: name of the resource, for example uelprdadx
  :param kustoDatabase: the database in which to query on the selected resource_group
  :param df_format: description of the return type, dfs for lightweighted spark dataframes, df for more heavy weighted pandas dataframes.
  
  :type query: str
  :type resource_name: str
  :type kustoDatabase: str
  :type df_format: str
  """
  
  supported_df_format = ['dfs', 'df']
  if df_format not in supported_df_format:
    sys.exit("Dataframe format not supported!")
    
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

class SQL_Connector:
  def __init__(self, full_server_name, db):
    auth_dict = get_azure_sql_authentication_dict(full_server_name, db)
    full_server_name = auth_dict['full_server_name']
    db_name = auth_dict['database']
    username = auth_dict['username']
    password = auth_dict['password']
    driver= '{ODBC Driver 17 for SQL Server}'
    self.odbc_driver = f'DRIVER={driver};SERVER={full_server_name};DATABASE={db_name};UID={username};PWD={password}'
    self.jdbcUrl = f"jdbc:sqlserver://{full_server_name};databaseName={auth_dict['database']};user={auth_dict['username']};password={auth_dict['password']};encrypt=true;hostNameInCertificate={full_server_name};"
    self.conn = None
    
  def open_connection(self, conn_type):
    supported_conn_types = ['odbc', 'jdbc']
    if conn_type not in supported_conn_types:
      sys.exit("ERROR - database connector not supported!")
    if conn_type == 'odbc':
      if pyodbc.drivers() != ['ODBC Driver 17 for SQL Server']:
        sys.exit("ERROR - driver type not present on system!")
      self.conn = pyodbc.connect(self.odbc_driver)
      
    if conn_type == 'jdbc':
      #do nothing, is handled by library.
      pass
    
  def close_connection(self):
    self.conn.close()
      
  def get_status(self):
    if type(self.conn) == type(None):
      print("Connection not yet established")
    else:
      print("Connection closed or open") 
      
class SubscriptionPlan(SQL_Connector):
  def __init__(self, full_server_name, db, channel=None):
    super().__init__(full_server_name, db)
    self.expected_input_data_type = type([])
    self.channel = channel
    
  def select_data(self, query):
    self.open_connection('jdbc')
    
    dfs = spark.read. \
      format("jdbc"). \
      option("url", self.jdbcUrl). \
      option("dbtable", 'subscription_plan'). \
      option("batchsize", 1000). \
      load()
    return dfs

  


# COMMAND ----------

query = "(select * from subscription_plan)"
dfs_subscription_plan_id = SubscriptionPlan('data-prd-sql', 'dc').select_data(query)
dfs_subscription_plan_id = dfs_subscription_plan_id.select(['id','country','channel','price','billing_frequency'])
del query

query = """
SELECT t1.CCY, t1.RATE 
FROM static_tables.forex_rates AS t1;
"""
#dfs_forex_rates = spark.sql(query)

del query

# COMMAND ----------

query = """
subscriptions_rawdatamod
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
| where startofday(datetime('2021-4-26')) == startofday(Date) and datetime_part("Hour",Date) >= 11
| summarize count = count()
"""


query = """
subscriptions
| where Date >= startofday(now()) and Date < startofday(now() + 1d) 
| summarize count = count()
"""

nr_records_today = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'dfs').select('count').first()[0]
if nr_records_today > 0:
  write = True
else:
  write = False
  dbutils.notebook.exit("TERMINATE - subscription table of today does not contain any data.")
  

# COMMAND ----------

query = """
subscriptions
| project Date, SubscriptionId = ['Subscription.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionStartDate = ['Subscription.SubscriptionStart'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], CustomerRegistrationCountry = ['Customer.RegistrationCountry'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id'], SubscriptionPlanBillingFrequency = ['SubscriptionPlan.BillingFrequency'], AdyenPayments = ['Adyen.Payment.Payments'], AdyenDisputes = ['Adyen.Dispute.Disputes'], RecurringEnabled = ['Subscription.RecurringEnabled'], AdyenFirstPaymentReceived = ['Adyen.Payment.FirstPaymentReceived'], AdyenTotalAmountInSettlementCurrency = ['Adyen.Payment.TotalAmountInSettlementCurrency'], SubscriptionPrice = ['SubscriptionPlan.Price'], SubscriptionCurrency = ['SubscriptionPlan.Currency'], SubscriptionState = ['Subscription.State'] 
| where startofday(datetime('2021-4-26'))== startofday(Date) and datetime_part("Hour",Date) >= 11
"""

query = """
subscriptions
| project Date, SubscriptionId = ['Subscription.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionStartDate = ['Subscription.SubscriptionStart'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], CustomerRegistrationCountry = ['Customer.RegistrationCountry'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id'], SubscriptionPlanBillingFrequency = ['SubscriptionPlan.BillingFrequency'], AdyenPayments = ['Adyen.Payment.Payments'], AdyenDisputes = ['Adyen.Dispute.Disputes'], RecurringEnabled = ['Subscription.RecurringEnabled'], AdyenFirstPaymentReceived = ['Adyen.Payment.FirstPaymentReceived'], AdyenTotalAmountInSettlementCurrency = ['Adyen.Payment.TotalAmountInSettlementCurrency'], SubscriptionPrice = ['SubscriptionPlan.Price'], SubscriptionCurrency = ['SubscriptionPlan.Currency'], SubscriptionState = ['Subscription.State'] 
| where Date >= startofday(now()) and Date < startofday(now() + 1d)
"""

dfs = run_data_explorer_query(query, resource_name ='uelprdadx', kustoDatabase = 'playground', df_format = 'dfs')

del query

# COMMAND ----------

def add_column_country(x1, x2, x3):
  #WL id = a495e56a-0139-58f3-9771-cec7690df76e
  """
  The country from which we consider the user to be. Based on subscription plan ID, and corrected as we can't seperate NL en BE users for WL.
  We correct by checking whether someone from WL has customer registration country BE.
  
  :param x1: Country based on the subscription_plan_id of the customer
  :param x2: Channel (ID)
  :param x3: Customer registration country
  :type x1: str
  :type x2: str
  :type x3: str
  
  Attention:
    - Affected by static_tables.subscription_plan_id, which is not automatically updated. 
  """
  if x2 == 'a495e56a-0139-58f3-9771-cec7690df76e' and x3 == 'BE':
    return 'BE'
  if x2 == '0ffffbf3-1d42-55f3-82c0-cb5029f55003':
    return 'NL'
  else: 
    return x1
  return 'ERROR'

dfs = dfs.join(dfs_subscription_plan_id.select(['id','country']),dfs['SubscriptionPlanId'] == dfs_subscription_plan_id['id'],'left')

Country = udf(add_column_country, StringType())
dfs = dfs.withColumn('Country',Country('country','Channel','CustomerRegistrationCountry'))

dfs = dfs.drop('subscription_id')

dfs = dfs.na.fill('NOT FOUND')

dfs.select(['Country']).show(10)

# COMMAND ----------

""" 
Attention:
  - Affected by static_tables.subscription_plan_id and static_tables.country_market which are not automatically updated.
"""

def add_country_market(x):
  country_to_market_mapper = {"NL":"Benelux"
           ,"BE":"Benelux"
           ,"LU":"Benelux"
           ,"GB":"UK&IE"
           ,"IE":"UK&IE"
           ,"UK":"UK&IE"
           ,"NZ":"Oceania"
           ,"AU":"Oceania"
           ,"SE":"Nordics"
           ,"NO":"Nordics"}
  try:
    return country_to_market_mapper[x]
  except Exception:
    return "COUNTRY {} NOT FOUND!".format(x)
Market = udf(lambda x: add_country_market(x), StringType())
dfs = dfs.withColumn('Market',Market('Country'))

# COMMAND ----------

def add_type(x1, x2):
  billingfrequency_to_period_mapper = {30: "Month"
           ,182: "Half"
           ,365: "Year"
           ,1: "Month"
           ,5: "Month"
           ,90: "Quarter"}
  # Hard code this ugly test plan as a quick fix.
  if x2 == '0-11-test-plan': 
    return 'Month'
  
  try:
    return billingfrequency_to_period_mapper[x1]
  except Exception:
    return "UNKNOWN BILLING FREQUENCY {}!".format(x1)
  
Type = udf(add_type, StringType())
dfs = dfs.withColumn('Type',Type('SubscriptionPlanBillingFrequency', 'SubscriptionPlanId'))

dfs.select(['Type']).toPandas().reset_index().groupby('Type').count()

# COMMAND ----------

def add_column_valid_record(x):
  """
  To mark customers who don't have a subscription create date. 
  
  :param x: SubscriptionCreatedDate column from subscription dataset
  :type x: date
  """
  if type(x) == type(None) or type(x) == type(np.nan) or x == '':
    return 'N' 
  return 'Y'

ValidRecord = udf(lambda x: add_column_valid_record(x), StringType())
dfs = dfs.withColumn('ValidRecord',ValidRecord('SubscriptionCreatedDate'))




# COMMAND ----------

def add_column_dc_bank(x):
  """
  Mark all customer id's that are showing in dc_sepa or dc_cc, indicating creditcard or sepa issues.
  
  :param x: CustomerId column from subscription dataset
  :type x: int  
  """
  try:
    return merchant_dict[x.replace('-','')]
  except KeyError:
    return 0
  return -100

def get_merchant_id_dict():
  dfs_dc_cc = spark.sql(query_dc_cc)
  dfs_dc_sepa = spark.sql(query_dc_sepa)
  
  dfs_dc_sepa = dfs_dc_sepa.withColumn("value", lit(1))
  dfs_dc_cc = dfs_dc_cc.withColumn("value", lit(1))
  
  df_dc_sepa = dfs_dc_sepa.toPandas()
  df_dc_sepa['merchant_reference'] = df_dc_sepa['merchant_reference'].apply(lambda x: x.replace("-",""))
  dict_dc_sepa = df_dc_sepa.set_index('merchant_reference').T.to_dict("int")['value']
  
  df_dc_cc = dfs_dc_cc.toPandas()
  df_dc_cc['merchant_reference'] = df_dc_cc['merchant_reference'].apply(lambda x: x.replace("-",""))
  dict_dc_cc = df_dc_cc.set_index('merchant_reference').T.to_dict("int")['value']
  
  dict_dc = {**dict_dc_sepa, **dict_dc_cc}

  return dict_dc

query_dc_cc = """
SELECT t1.`Merchant Reference` AS merchant_reference
FROM finance.dc_cc AS t1
GROUP BY t1.`Merchant Reference`;
"""

query_dc_sepa =""" 
SELECT t1.`Merchant Reference` AS merchant_reference
FROM finance.dc_sepa AS t1
GROUP BY t1.`Merchant Reference`;
"""

merchant_dict = get_merchant_id_dict()
DCBank = udf(lambda x: add_column_dc_bank(x), IntegerType())
dfs = dfs.withColumn("DCBank", DCBank('SubscriptionId'))

del merchant_dict

# COMMAND ----------

def add_column_trial_only_user(x1, x2):
  """
  Mark users who only have been in the trial period.
  
  :param x1: Payment.payments column in subscription dataset
  :param x2: DCBank, manually created column in past cell 
  :type x1: int, None values present
  :type x2: int, boolean
  """
  if type(x1) == type(None):
    x1 = 0
  if x1 > 0 and x2 == 0:
    return "N"
  
  return "Y" 

TrialOnlyUser = udf(add_column_trial_only_user, StringType())
dfs = dfs.withColumn("TrialOnlyUser", TrialOnlyUser("AdyenPayments","DCBank"))

# COMMAND ----------

def add_column_trial_chargeback(x1, x2, x3):
  """
  Mark subscriptions that applied for a chargeback after the trial period.
  
  :param x1: payment.payments from subscription table in data explorer
  :param x2: dispute.disputes from subscription table in data explorer
  :param x3: trial only user, as calculated in previous cells
  
  :type x1: int, None values present
  :type x2: int, None values present
  :type x3: str
  """
  if type(x1) == type(None):
    x1 = 0
  if type(x2) == type(None):
    x2 = 0
  if int(x1) - int(x2) <= 0 and x3 == 'N':
    return 'Y'
  
  return 'N'

TrialChargeback = udf(add_column_trial_chargeback, StringType())
dfs = dfs.withColumn("TrialChargeback", TrialChargeback("AdyenPayments", "AdyenDisputes", "TrialOnlyUser"))

# COMMAND ----------

def add_column_trial_churn(x1, x2):
  """
  Mark the type of customer - CB (ChargeBack), Paid or Regular. 
  
  :param x1: trial chargeback column as calculated in previous cells
  :param x2: trial only user as calculated in previous cells
  
  :type x1: str
  :type x2: str
  """
  if x1 == "N" and x2 == "Y":
    return "Regular"
  elif x1 == "Y":
    return "CB"
  return "Paid"

TrialChurn = udf(add_column_trial_churn, StringType())
dfs = dfs.withColumn("TrialChurn", TrialChurn("TrialChargeback", "TrialOnlyUser"))

# COMMAND ----------

def add_column_cohort_eva_date(x1, x2, x3, x4):
  """
  Use as actual start of paid lifetime. Takes into account: free months, vouchers.
  
  :param x1: trial chargeback as calculated in previous column
  :param x2: trial only user as calculated in previous column
  :param x3: created date of subscription in subscription table in data explorer
  :param x4: end date of subscription in subscription table in data explorer
  
  :type x1: str
  :type x2: str
  :type x3: timestamp
  :type x4: timestamp
  """
  if x1 == 'Y':
    return datetime.strptime(str(x3.date()), '%Y-%m-%d')
  elif x2 == 'Y':
    return datetime.strptime(str(x3.date()), '%Y-%m-%d')
  return datetime.strptime(str(x4.date()), '%Y-%m-%d')

CohortEvaDate = udf(add_column_cohort_eva_date, TimestampType())
dfs = dfs.withColumn("CohortEvaDate", CohortEvaDate("TrialChargeback","TrialOnlyUser",'SubscriptionCreatedDate', "SubscriptionEndDate"))

# COMMAND ----------

def add_column_active_user(x1):
  """
  Mark whether a subscription is still active, or whether the end_date is in the past.
  
  :param x1: subscription end date from subscription table in data explorer.
  :type x1: timestamp
  """
  if x1.date() >= datetime.now().date():
    return "Y"
  return "N"

ActiveUser = udf(lambda x: add_column_active_user(x), StringType())
dfs = dfs.withColumn("ActiveUser", ActiveUser("SubscriptionEndDate"))

# COMMAND ----------

def add_column_active_paid(x1, x2, x3, x4):
  """
  Mark whether a subsription is still active, and in the paid phase.
  
  :param x1: trial chargeback as calculated in previous cells
  :param x2: active user as calculated in previous cells
  :param x3: payment.payments from subscription table in data explorer 
  :param x4: dc bank as calculated in previous cells
  
  :type x1: str
  :type x2: str
  :type x3: int
  :type x4: int, boolean
  """
  if type(x3) == type(None):
    x3 = 0
  if x1 == "Y":
    return 0
  elif x2 == "Y" and x3 > 0 and x4 == 0:
    return 1
  return 0
  
ActivePaid = udf(add_column_active_paid, IntegerType())
dfs = dfs.withColumn("ActivePaid", ActivePaid("TrialChargeback", "ActiveUser", "AdyenPayments", "DcBank"))

# COMMAND ----------

def add_column_active_trial(x1, x2):
  """
  Mark whether user is currently in the trial phase.
  
  :param x1: active user variable calculated in previous cells
  :param x2: active paid variable calculated in previous cells
  
  :type x1: str
  :type x2: int
  """
  if x1 == "Y" and x2 != 1:
    return 1
  return 0

ActiveTrial = udf(add_column_active_trial, IntegerType())
dfs = dfs.withColumn("ActiveTrial", ActiveTrial("ActiveUser","ActivePaid"))

# COMMAND ----------

def add_column_active_true_paid(x1, x2, x3):
  """
  Whether subscription is considered 'Paid', as well as being considered 'recurring', indicating the user is not inquired to leave soon.
  
  :param x1: trial chargeback as calculated in previous cells
  :param x2: recurring_enabled from subscription data in data explorer
  :param x3: active paid as calculated in previous cells
  
  :type x1: str
  :type x2: boolean
  :type x3: int
  """
  if x1 == "Y":
    return 0
  elif x2 == True and x3 == 1:
    return 1
  return 0

ActiveTruePaid = udf(add_column_active_true_paid, IntegerType())
dfs = dfs.withColumn("ActiveTruePaid", ActiveTruePaid("TrialChargeback", "RecurringEnabled", "ActivePaid"))

# COMMAND ----------

def add_column_active_true_trial(x1, x2):
  """
  Whether subscription is considered 'Trial', as well as being considered 'recurring', indicating the user is not inquired to leave soon.
  
  :param x1: active trial as calculated in previous cells
  :param x2: recurring_enabled from subscription data in data explorer
  
  :type x1: int
  :type x2: boolean
  """
  if x1 == 1 and x2 == True:
    return 1
  return 0
ActiveTrueTrial = udf(add_column_active_true_trial, IntegerType())
dfs = dfs.withColumn("ActiveTrueTrial", ActiveTrueTrial("ActiveTrial", "RecurringEnabled"))

# COMMAND ----------

def add_column_active_cancelled_paid(x1, x2, x3):
  """
  Marks whether a paid user is currently active, but likely to churn due to the fact that the person switched off the recurring payments.
  
  :param x1: trial chargeback as calculated in previous cells
  :param x2: recurring_enabled from subscription data in data explorer
  :param x3: active paid as calculated in previous cells
  
  :type x1: str
  :type x2: boolean
  :type x3: int
  """
  if x1 == "Y":
    return 0
  elif x2 == False and x3 == 1:
    return 1
  return 0

ActiveCancelledPaid = udf(add_column_active_cancelled_paid, IntegerType())
dfs = dfs.withColumn("ActiveCancelledPaid", ActiveCancelledPaid("TrialChargeback", "RecurringEnabled", "ActivePaid"))

# COMMAND ----------

def add_column_active_cancelled_trial(x1, x2):
  """
  Marks whether a trial user is currently active, but likely to churn due to the fact that the person switched off the recurring payments.
  
  :param x1: recurring_enabled from subscription data in data explorer
  :param x2: active trial as calculated in previous cells
  
  :type x1: boolean
  :type x2: int
  """
  if x1 == False and x2 == 1:
    return 1
  return 0

ActiveCancelledTrial = udf(add_column_active_cancelled_trial, IntegerType())
dfs = dfs.withColumn("ActiveCancelledTrial", ActiveCancelledTrial("RecurringEnabled", "ActiveTrial"))

# COMMAND ----------

def add_column_records(x1):
  """
  Marks whether the channel is empty.
  
  :param x1: channel from subscription data in data explorer
  
  :type x1: str
  """
  if type(x1) == type(None) or type(x1) == type(np.nan) or x1 == '' or type(x1) != str:
    return 0
  return 1

Records = udf(lambda x: add_column_records(x), IntegerType())
dfs = dfs.withColumn("Records", Records("Channel"))


# COMMAND ----------

def add_column_total_paid(x1, x2):
  """
  Marks whether a subscriber has been considered paid in his lifetime.
  
  :param x1: trial chargeback as calculated in previous cells
  :param x2: trial only user as calculated in previous cells
  
  :type x1: str
  :type x2: str
  
  """
  if x1 == "Y":
    return 0
  elif x2 == "Y":
    return 0
  return 1

TotalPaid = udf(add_column_total_paid, IntegerType())
dfs = dfs.withColumn("TotalPaid", TotalPaid("TrialChargeback","TrialOnlyUser"))


# COMMAND ----------

def add_column_total_trial(x1, x2):
  """
  Marks whether a subscriber has only been considered trial in his lifetime.
  
  :param x1: records as calculated in previous cells
  :param x2: total paid user as calculated in previous cells
  
  :type x1: int
  :type x2: int
  """
  return x1 - x2

TotalTrial = udf(add_column_total_trial, IntegerType())
dfs = dfs.withColumn("TotalTrial", TotalTrial("Records","TotalPaid"))


# COMMAND ----------

def add_column_regular_trial_churn(x1, x2):
  """
  Marks whether a subscriber has only been considered trial in his lifetime.
  
  :param x1: trial only user as calculated in previous cells
  :param x2: trial chargeback as calculated in previous cells
  
  :type x1: str
  :type x2: str 
  """
  if x1 == "Y" and x2 == "N":
    return 1
  return 0

RegularTrialChurn = udf(add_column_regular_trial_churn, IntegerType())
dfs = dfs.withColumn("RegularTrialChurn", RegularTrialChurn("TrialOnlyUser","TrialChargeback"))

# COMMAND ----------

def add_column_chargeback_trial_churn(x1, x2):
  """
  Marks whether a subscriber has only been considered trial in his lifetime.
  
  :param x1: total trial as calculated in previous cells
  :param x2: regular trial churn as calculated in previous cells
  
  :type x1: int
  :type x2: int
  """
  return x1 - x2

ChargebackTrialChurn = udf(add_column_chargeback_trial_churn, IntegerType())
dfs = dfs.withColumn("ChargebackTrialChurn", ChargebackTrialChurn("TotalTrial","RegularTrialChurn"))


# COMMAND ----------

def round_half_up(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n*multiplier + 0.5) / multiplier
  
def get_first_day_of_month(date):
  return datetime(date.year,date.month,1,0,0,0).date()

def add_column_paid_month(x1, x2):
  """
  The absolute number of months a subscriber is considered a paid customer.
  
  :param x1: cohort eva date as calculated in previous cells 
  :param x2: subscription created date from the subscription table in data explorer
  
  :type x1: timestamp
  :type x2: timestamp
  """
  x1 = x1.date()
  x2 = x2.date()
  
  difference = get_first_day_of_month(x1) - get_first_day_of_month(x2)
  return int(round_half_up(difference.days / 30,0))
  
PaidMonth = udf(add_column_paid_month, IntegerType())
dfs = dfs.withColumn("PaidMonth", PaidMonth("CohortEvaDate", "SubscriptionCreatedDate"))

# COMMAND ----------

def add_column_delta():
  """
  This column is blank, and functions as placeholder.
  """
  
  return 0

Delta = udf(add_column_delta, IntegerType())
dfs = dfs.withColumn("Delta", Delta())

# COMMAND ----------

def add_column_historic_subscription_start_date(x1, x2, x3):
  """
  The historic date a user got their subscription. If paid user the day after their first payment, else subscription start date.
  
  :param x1: trial churn as calculated in previous column
  :param x2: first payment received date of subscription in subscription table in data explorer
  :param x3: start date of subscription in subscription table in data explorer
  
  :type x1: str
  :type x2: timestamp
  :type x3: timestamp
  """

  if x1 == 'Paid':
    if type(x2) == type(None):
      return datetime.strptime("1900-1-1", '%Y-%m-%d') 
    return datetime.strptime(str(x2.date()), '%Y-%m-%d') + timedelta(days=1)
  else:
    if type(x3) == type(None):
      return datetime.strptime("1900-1-1", '%Y-%m-%d')
    return datetime.strptime(str(x3.date()), '%Y-%m-%d')

HistoricSubStartDate = udf(add_column_historic_subscription_start_date, TimestampType())
dfs = dfs.withColumn("HistoricSubscriptionStartDate", HistoricSubStartDate("TrialChurn","AdyenFirstPaymentReceived",'SubscriptionStartDate'))

# COMMAND ----------

def add_column_price_per_cycle(x1, x2):
  """
  The average price paid per cycle in the settlement currency of the user.
  
  :param x1: total amount of settlement currency gotten from subscription in subscription table in data explorer
  :param x2: number of payments of subscription in subscription table in data explorer
  
  :type x1: float
  :type x2: int
  """
  if type(x2) == type(None):
    x2 = 0.0
  if type(x1) == type(None):
    x1 = 0.0
  if x2 > 0:
    return x1 / x2
  else:
    return 0.0

PricePerCycle = udf(add_column_price_per_cycle, FloatType())
dfs = dfs.withColumn("PricePerCycle", PricePerCycle("AdyenTotalAmountInSettlementCurrency","AdyenPayments"))

# COMMAND ----------

def add_rate(x):
  ccy_to_rate_mapper = {
            "EUR": 1.0
           ,"NOK": 0.095
           ,"SEK": 0.099
           ,"GBP": 1.113
           ,"AUD": 0.629
           ,"NZD": 0.588
           ,"USD": 0.815}
  try:
    return ccy_to_rate_mapper[x]
  except Exception as e:
    print(e)
    return "NO RATES KNOWN FOR {}".format(x)
  
def add_column_plan_price(x1, x2):
  """
  The average price paid per cycle in the settlement currency of the user.
  
  :param x1: price of subscription in subscription table in data explorer
  :param x2: forex rate of corresponding currency from the forex_rates table in static tables
  
  :type x1: float
  :type x2: float
  """
  if type(x1) == type(None):
    x1 = 0.0
  if type(x2) == type(None):
    x2 = 0.0
  return x1*x2

Rate = udf(add_rate, FloatType())
dfs = dfs.withColumn("Rate", Rate("SubscriptionCurrency"))

PlanPrice = udf(add_column_plan_price, FloatType())
dfs = dfs.withColumn("PlanPrice", PlanPrice("SubscriptionPrice","RATE"))

# COMMAND ----------

added_cols = ['Country','Market','Type','ValidRecord','TrialOnlyUser','TrialChargeback','TrialChurn'
             , 'CohortEvaDate', 'ActiveUser','ActivePaid','ActiveTrial','ActiveTruePaid','ActiveTrueTrial'
              ,'ActiveCancelledPaid','ActiveCancelledTrial','Records','TotalPaid'
              ,'TotalTrial','RegularTrialChurn','ChargebackTrialChurn','PaidMonth','Delta','DCBank'
              , 'HistoricSubscriptionStartDate', 'PricePerCycle','PlanPrice'
             ]
base_cols = ['Date', 'SubscriptionId']
dfs_final = dfs.select(base_cols + added_cols)
dfs_final = dfs_final.withColumn("Created", lit(str(datetime.now(pytz.timezone('Europe/Amsterdam')))))

print("Print basic info (any indications for missing data in dimension tables):")
print(dfs_final.filter(dfs_final['Country'].contains("NOT FOUND")).count())
print(dfs_final.filter(dfs_final['Market'].contains("NOT FOUND")).count())
print(dfs.filter(dfs['Country'].contains("NOT FOUND")).select("SubscriptionPlanId").toPandas())

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

write_to_data_explorer(dfs_final, 'subscriptions_rawdatamod', resource_name, kustoDatabase, write)

# COMMAND ----------

dbutils.notebook.exit("success")
