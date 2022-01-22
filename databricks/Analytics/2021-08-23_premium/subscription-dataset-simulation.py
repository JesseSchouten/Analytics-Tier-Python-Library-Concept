# Databricks notebook source
# Python standard libraries
import sys
from datetime import datetime, timedelta
import math

# Python community libraries
import pandas as pd
import numpy as np
import random
import uuid
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType, BooleanType, StructType, StructField
from scipy.stats import poisson

# Python DutchChannels libraries
from dc_azure_toolkit import cosmos_db, adx

# COMMAND ----------

SIMULATION_OPTIONS = {
  "days": 365,
  "churnRatePerDay": 1/90,
  "meanNewTrialsPerDay": 4,
  "meanNewVouchersPerDay": 1/5,
  "basicToPremiumRatePerDay": 1/30,
  "premiumToBasicRatePerDay": 1/60,
}

DEBUG_MODE = False

# COMMAND ----------

CHANNEL_DICT = {
    'nfn': 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d',
    'hor': '0ffffbf3-1d42-55f3-82c0-cb5029f55003',
    'vg': '25b748df-e99f-5ec9-aea5-967a73c88381',
    'lov': 'a495e56a-0139-58f3-9771-cec7690df76e'
}

PRICEPLAN_DICT = {
  '0-11-nfn-month-plan-nl': 7.95,
  '0-11-month-plan': 6.95,
  '0-11-nfn-month-plan-gb': 5.99,
  '0-11-lov-month-plan-se': 85.0,
  '0-11-horrify-premium': 6.95,
  '0-11-lov-month-plan-nl': 6.95,
  '0-11-nfn-month-plan-no': 89.0,
  '0-11-nfn-year-plan': 79.95,
  '0-11-nfn-year-plan-nl': 79.95,
  '0-11-nfn-month-plan-se': 89.0,
  '0-11-nfn-month-plan-au': 7.95,
  '0-11-lov-month-plan-no': 79.0,
  '0-11-nfn-month-plan-nz': 9.95,
  '0-11-vg-month-plan-nl': 9.95,
  '0-11-nfn-halfyear-plan': 39.95,
  '0-11-voorstelling-gemist-premium': 6.95,
  '0-11-nfn-year-plan-au': 79.95,
  '0-11-nfn-year-plan-gb': 59.99,
  '0-11-nfn-year-plan-no': 890.0,
  '0-11-lov-halfyear-plan': 35.95,
  '0-11-lov-year-plan': 69.95,
  '0-11-lov-quarter-plan': 6.95,
  '0-11-nfn-halfyear-plan-gb': 24.99,
  '0-11-nfn-month-plan-ie': 6.95,
  '0-11-nfn-month-plan-au-test': 9.95,
  '0-11-nfn-halfyear-plan-no': 395.0,
  '0-11-nfn-year-plan-se': 890.0,
  '0-11-nfn-month-plan-no-test': 89.0,
  '0-11-nfn-year-plan-nz': 99.95,
  '0-11-nfn-month-plan-gb-test': 5.99,
  '0-11-hor-halfyear-plan': 34.95,
  '0-11-lov-month-plan-be-test': 6.95,
  '0-11-nfn-month-plan-se-test': 89.0,
  '0-11-vg-year-plan-nl': 99.95,
  '0-11-lov-year-plan-nl': 69.95,
  '0-11-nfn-halfyear-plan-se': 425.0,
  '0-11-lov-month-plan-be': 6.95,
  '0-11-nfn-year-plan-au-test': 99.95,
  '0-11-lov-year-plan-se': 850.0,
  '0-11-hor-year-plan': 69.95,
  '0-11-lov-year-plan-no': 790.0,
  '0-11-nfn-year-plan-ie': 69.95,
  '0-11-nfn-year-plan-no-test': 890.0,
  '0-11-vg-halfyear-plan': 34.95,
  '0-11-nfn-year-plan-gb-test': 59.99,
  '0-11-nfn-halfyear-plan-ie': 29.95,
  '0-11-lov-year-plan-be-test': 69.95,
  '0-11-nfn-year-plan-se-test': 890.0,
  '0-11-vg-year-plan': 69.95,
  '0-11-tt-year-plan-promo-ctw': 119.4,
  '0-11-lov-year-plan-be': 69.95,
  '0-11-nfn-month-plan-ie-test': 6.95,
  '0-11-tt-year-plan-promo-bgl': 119.4,
  '0-11-nfn-year-plan-ie-test': 69.95,
  '0-11-lov-month-plan': 6.95,
  '0-11-svodsubsc_1660914786': 7.95,
  '0-11-dc-test-plan': 1.0,
  '0-11-svodsubsc_1025361845': 1.0,
  '0-11-nfn-month-plan-gb-premium': 7.99,
  '0-11-nfn-month-plan-nl-premium': 9.50,
  '0-11-nfn-month-plan-ie-premium': 7.99,
  '0-11-nfn-month-plan-no-premium': 100.0,
  '0-11-nfn-month-plan-se-premium': 100.0,
  '0-11-nfn-month-plan-au-premium': 9.50,
  '0-11-nfn-month-plan-nz-premium': 9.50,
  '0-11-nfn-year-plan-gb-premium': 80.00,
  '0-11-nfn-year-plan-nl-premium': 100.0,
  '0-11-nfn-year-plan-ie-premium': 80.00,
  '0-11-nfn-year-plan-no-premium': 850.0,
  '0-11-nfn-year-plan-se-premium': 850.0,
  '0-11-nfn-year-plan-au-premium': 100.0,
  '0-11-nfn-year-plan-nz-premium': 100.0,
  '0-11-lov-month-plan-nl-premium': 9.50,
  '0-11-lov-month-plan-se-premium': 100.0,
  '0-11-lov-month-plan-no-premium': 100.0,
  '0-11-lov-month-plan-be-premium': 9.50,
  '0-11-lov-year-plan-nl-premium': 80.0,
  '0-11-lov-year-plan-se-premium': 850.0,
  '0-11-lov-year-plan-no-premium': 850.0,
  '0-11-lov-year-plan-be-premium': 80.0,
  '0-11-nfn-month-plan-premium-au-test': 9.50,
  '0-11-nfn-year-plan-premium-au-test': 100.0
}

CCY_TO_RATE_DICT = {
            "EUR": 1.0
           ,"NOK": 0.095
           ,"SEK": 0.099
           ,"GBP": 1.113
           ,"AUD": 0.629
           ,"NZD": 0.588
           ,"USD": 0.815}

PREMIUM_TO_BASIC_MAPPER = {
  '0-11-nfn-month-plan-gb-premium': '0-11-nfn-month-plan-gb',
  '0-11-nfn-month-plan-nl-premium': '0-11-nfn-month-plan-nl',
  '0-11-nfn-month-plan-ie-premium': '0-11-nfn-month-plan-ie',
  '0-11-nfn-month-plan-no-premium': '0-11-nfn-month-plan-no',
  '0-11-nfn-month-plan-se-premium': '0-11-nfn-month-plan-se',
  '0-11-nfn-month-plan-au-premium': '0-11-nfn-month-plan-au',
  '0-11-nfn-month-plan-nz-premium': '0-11-nfn-month-plan-nz',
  '0-11-nfn-year-plan-gb-premium': '0-11-nfn-year-plan-gb',
  '0-11-nfn-year-plan-nl-premium': '0-11-nfn-year-plan-nl',
  '0-11-nfn-year-plan-ie-premium': '0-11-nfn-year-plan-ie',
  '0-11-nfn-year-plan-no-premium': '0-11-nfn-year-plan-no',
  '0-11-nfn-year-plan-se-premium': '0-11-nfn-year-plan-se',
  '0-11-nfn-year-plan-au-premium': '0-11-nfn-year-plan-au',
  '0-11-nfn-year-plan-nz-premium': '0-11-nfn-year-plan-nz',
  '0-11-lov-month-plan-nl-premium': '0-11-lov-month-plan-nl',
  '0-11-lov-month-plan-se-premium': '0-11-lov-month-plan-se',
  '0-11-lov-month-plan-no-premium': '0-11-lov-month-plan-no',
  '0-11-lov-month-plan-be-premium': '0-11-lov-month-plan-be',
  '0-11-lov-year-plan-nl-premium': '0-11-lov-year-plan-nl',
  '0-11-lov-year-plan-se-premium': '0-11-lov-year-plan-se',
  '0-11-lov-year-plan-no-premium': '0-11-lov-year-plan-no',
  '0-11-lov-year-plan-be-premium': '0-11-lov-year-plan-be'
}

BASIC_TO_PREMIUM_MAPPER = {
  '0-11-nfn-month-plan-gb': '0-11-nfn-month-plan-gb-premium',
  '0-11-nfn-month-plan-nl': '0-11-nfn-month-plan-nl-premium',
  '0-11-nfn-month-plan-ie': '0-11-nfn-month-plan-ie-premium',
  '0-11-nfn-month-plan-no': '0-11-nfn-month-plan-no-premium',
  '0-11-nfn-month-plan-se': '0-11-nfn-month-plan-se-premium',
  '0-11-nfn-month-plan-au': '0-11-nfn-month-plan-au-premium',
  '0-11-nfn-month-plan-nz': '0-11-nfn-month-plan-nz-premium',
  '0-11-nfn-year-plan-gb': '0-11-nfn-year-plan-gb-premium',
  '0-11-nfn-year-plan-nl': '0-11-nfn-year-plan-nl-premium',
  '0-11-nfn-year-plan-ie': '0-11-nfn-year-plan-ie-premium',
  '0-11-nfn-year-plan-no': '0-11-nfn-year-plan-no-premium',
  '0-11-nfn-year-plan-se': '0-11-nfn-year-plan-se-premium',
  '0-11-nfn-year-plan-au': '0-11-nfn-year-plan-au-premium',
  '0-11-nfn-year-plan-nz': '0-11-nfn-year-plan-nz-premium',
  '0-11-lov-month-plan-nl': '0-11-lov-month-plan-nl-premium',
  '0-11-lov-month-plan-se': '0-11-lov-month-plan-se-premium',
  '0-11-lov-month-plan-no': '0-11-lov-month-plan-no-premium',
  '0-11-lov-month-plan-be': '0-11-lov-month-plan-be-premium',
  '0-11-lov-year-plan-nl': '0-11-lov-year-plan-nl-premium',
  '0-11-lov-year-plan-se': '0-11-lov-year-plan-se-premium',
  '0-11-lov-year-plan-no': '0-11-lov-year-plan-no-premium',
  '0-11-lov-year-plan-be': '0-11-lov-year-plan-be-premium',
  '0-11-month-plan': '0-11-lov-month-plan-nl-premium',
  '0-11-nfn-month-plan-au-test': '0-11-nfn-month-plan-au-premium',
  '0-11-nfn-month-plan-no-test': '0-11-nfn-month-plan-no-premium',
  '0-11-nfn-month-plan-gb-test': '0-11-nfn-month-plan-gb-premium',
  '0-11-lov-month-plan-be-test': '0-11-lov-month-plan-be-premium',
  '0-11-nfn-month-plan-se-test': '0-11-nfn-month-plan-se-premium',
  '0-11-nfn-year-plan-au-test': '0-11-nfn-year-plan-au-premium',
  '0-11-nfn-year-plan-no-test': '0-11-nfn-year-plan-no-premium',
  '0-11-nfn-year-plan-gb-test': '0-11-nfn-year-plan-gb-premium',
  '0-11-lov-year-plan-be-test': '0-11-lov-year-plan-be-premium',
  '0-11-nfn-year-plan-se-test': '0-11-nfn-year-plan-se-premium',
  '0-11-nfn-month-plan-ie-test': '0-11-nfn-month-plan-ie-premium',
  '0-11-nfn-year-plan-ie-test': '0-11-nfn-year-plan-ie-premium',
  '0-11-svodsubsc_1660914786': '0-11-nfn-month-plan-nl-premium'
}

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
    client_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-clientid")
    client_secret = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-clientsecret")
    authority_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-authorityid")
    authentication_dict = {
      'cluster': cluster,
      'client_id': client_id,
      'client_secret': client_secret,
      'authority_id':authority_id
    }
    
  elif resource_name == 'ueldevadx':
    cluster = "https://ueldevadx.westeurope.kusto.windows.net" # id 
    client_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-dev-adx-clientid")
    client_secret = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-dev-adx-clientsecret")
    authority_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-dev-adx-authorityid")
    authentication_dict = {
      'cluster': cluster,
      'client_id': client_id,
      'client_secret': client_secret,
      'authority_id':authority_id
    }
  return authentication_dict

query = """
subscriptions
| project Date, SubscriptionId = ['Subscription.Id'], SubscriptionCreatedDate = ['Subscription.CreatedDate'], SubscriptionStartDate = ['Subscription.SubscriptionStart'], SubscriptionEndDate = ['Subscription.SubscriptionEnd'], CustomerRegistrationCountry = ['Customer.RegistrationCountry'], Channel = Channel, SubscriptionPlanId = ['SubscriptionPlan.Id'], SubscriptionPlanBillingFrequency = ['SubscriptionPlan.BillingFrequency'], AdyenPayments = ['Adyen.Payment.Payments'], AdyenDisputes = ['Adyen.Dispute.Disputes'], RecurringEnabled = ['Subscription.RecurringEnabled'],AdyenFirstPayment = ['Adyen.Payment.FirstPayment'], AdyenFirstPaymentReceived = ['Adyen.Payment.FirstPaymentReceived'], AdyenPaymentTotalAmountInSettlementCurrency = ['Adyen.Payment.TotalAmountInSettlementCurrency'], SubscriptionPrice = ['SubscriptionPlan.Price'], SubscriptionCurrency = ['SubscriptionPlan.Currency'], SubscriptionState = ['Subscription.State'] 
| where Date >= startofday(now()) and Date < startofday(now() + 1d) and (Channel == 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d' or Channel == 'a495e56a-0139-58f3-9771-cec7690df76e')
"""

query = """
subscriptions
| project Date
, ['Channel'] = Channel
, ['CustomerRegistrationCountry'] = ['Customer.RegistrationCountry']
, ['SubscriptionId'] = ['Subscription.Id']
, ['CustomerId'] = ['Customer.Id']
, ['SubscriptionCreatedDate'] = ['Subscription.CreatedDate']
, ['SubscriptionStartDate'] = ['Subscription.SubscriptionStart']
, ['SubscriptionEndDate'] = ['Subscription.SubscriptionEnd']
, ['RecurringEnabled'] = ['Subscription.RecurringEnabled']
, ['SubscriptionFreeTrial'] = ['Subscription.FreeTrial']
, ['SubscriptionImpliedFreeTrial'] = ['Subscription.ImpliedFreeTrial']
, ['SubscriptionPlanId'] = ['SubscriptionPlan.Id']
, ['SubscriptionPlanBillingCycleType'] = ['SubscriptionPlan.BillingCycleType']
, ['SubscriptionPlanBillingFrequency'] = ['SubscriptionPlan.BillingFrequency']
, ['SubscriptionPrice'] = ['SubscriptionPlan.Price']
, ['SubscriptionCurrency'] = ['SubscriptionPlan.Currency']
, ['SubscriptionPlanFreeTrial'] = ['SubscriptionPlan.FreeTrial']
, ['AdyenTrialDisputes'] = ['Adyen.Payment.TrialPayments']
, ['AdyenDisputes'] = ['Adyen.Dispute.Disputes']
, ['AdyenDisputeTotalAmount'] = ['Adyen.Dispute.TotalAmount']
, ['AdyenDisputeFirstDispute'] = ['Adyen.Dispute.FirstDispute']
, ['AdyenDisputeVoluntaryChargebacks'] = ['Adyen.Dispute.VoluntaryChargebacks']
, ['AdyenDisputeInvoluntaryChargebacks'] = ['Adyen.Dispute.InvoluntaryChargebacks']
, ['AdyenTrialPayments'] = ['Adyen.Payment.TrialPayments']
, ['AdyenPayments'] = ['Adyen.Payment.Payments']
, ['AdyenPaymentTotalAmount'] = ['Adyen.Payment.TotalAmount']
, ['AdyenPaymentTotalAmountInSettlementCurrency'] = ['Adyen.Payment.TotalAmountInSettlementCurrency']
, ['AdyenPaymentAverageCcyFxRate'] = ['Adyen.Payment.AverageCcyFxRate']
, ['AdyenPaymentFirstPayment'] = ['Adyen.Payment.FirstPayment']
, ['AdyenPaymentFirstPaymentReceived'] = ['Adyen.Payment.FirstPaymentReceived']
, ['AdyenPaymentFirstPaymentAuthorised'] = ['Adyen.Payment.FirstPaymentAuthorised']
, ['AdyenPaymentFirstPaymentSentForSettled'] = ['Adyen.Payment.FirstPaymentSentForSettled']
, ['AdyenPaymentFirstPaymentSettled'] = ['Adyen.Payment.FirstPaymentSettled']
, ['AdyenPaymentLastRecurringPaymentMethod'] = ['Adyen.Payment.LastRecurringPaymentMethod']
, ['AdyenPaymentSumCommission'] = ['Adyen.Payment.SumCommission']
, ['AdyenPaymentSumMarkup'] = ['Adyen.Payment.SumMarkup']
, ['AdyenPaymentSumSchemeFee'] = ['Adyen.Payment.SumSchemeFee']
, ['AdyenPaymentSumInterchange'] = ['Adyen.Payment.SumInterchange']
, ['SubscriptionIdentifier'] = ['Subscription.Identifier']
, ['SubscriptionState'] = ['Subscription.State']
, ['SubscriptionNewPlanId'] = ['Subscription.NewPlanId']
| where Date >= startofday(now()) and Date < startofday(now() + 1d) and SubscriptionPlanId != '0-11-nfn-year-plan'
"""
# and SubscriptionPlanId != '0-11-nfn-year-plan' and (Channel == 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d' or Channel == 'a495e56a-0139-58f3-9771-cec7690df76e')

authentication_dict = get_data_explorer_authentication_dict('uelprdadx')
df = adx.select(query, authentication_dict, kustoDatabase = 'playground', df_format = 'df')
df['SubscriptionCreatedDate'] = df['SubscriptionCreatedDate'].apply(lambda x: x.date())
df['SubscriptionStartDate'] = df['SubscriptionStartDate'].apply(lambda x: x.date())
df['SubscriptionEndDate'] = df['SubscriptionEndDate'].apply(lambda x: x.date())
df = df.set_index("SubscriptionId")

print("Length before removing duplicates: {}".format(len(df)))
del df['Date']
df = df.drop_duplicates()
df['Date'] = datetime.now().date()
print("Length after removing duplicates: {}".format(len(df)))

# COMMAND ----------

# Python DutchChannels libraries
from dc_azure_toolkit import cosmos_db

def get_authentication_dict_cosmosdb(resource_name):
  """
  Get the dictionary with required authentication credentials for connection to a given cosmos database instance.
  
  :param resource_name: name of the cosmosDB resource.
  :type resource_name: str
  
  """
  if resource_name not in ['uds-prd-db-account']:
    return None
  if resource_name == 'uds-prd-db-account':
    result = {'account_url' : 'https://uds-prd-db-account.documents.azure.com:443/'
             ,'account_key': dbutils.secrets.get(scope = "data-prd-keyvault", key = "uds-prd-db-account-accountkey")
             }
  return result

#SubscriptionPlanId, SubscriptionPlanBillingCycleType, SubscriptionPlanBillingFrequency, SubscriptionPrice, SubscriptionCurrency, SubscriptionPlanFreeTrial
auth_dict = get_authentication_dict_cosmosdb('uds-prd-db-account')
query = """
SELECT 
    c.channel AS channel,
    c.title AS title,
    c.id AS SubscriptionPlanId,
    c.billing_cycle_type AS SubscriptionPlanBillingCycleType,
    c.billing_frequency AS SubscriptionPlanBillingFrequency,
    c.price AS SubscriptionPrice,
    c.currency AS SubscriptionCurrency,
    c.free_trial AS SubscriptionPlanFreeTrial
FROM c 
"""
# By creating a mock-df (df_subscription_plan_id_2), we attempt to easily add new non existing subscription-plan-ids for the premium. 
# The premium subscription_plan_ids should be identical to the previous id's, except for price.
df_subscription_plan_id = cosmos_db.select(query, auth_dict, 'uds-db', 'axinom-subscription-plan')
df_subscription_plan_id_2 = df_subscription_plan_id.copy()
df_subscription_plan_id_2['SubscriptionPlanId'] = df_subscription_plan_id_2['SubscriptionPlanId'] + '-premium'
df_subscription_plan_id_2 = df_subscription_plan_id_2[df_subscription_plan_id_2['SubscriptionPlanId'].isin(list(PRICEPLAN_DICT.keys()))]
df_subscription_plan_id_2['SubscriptionPrice'] = df_subscription_plan_id_2['SubscriptionPlanId'].apply(lambda x: PRICEPLAN_DICT[x])
df_subscription_plan_id = pd.concat((df_subscription_plan_id,df_subscription_plan_id_2))
SUBSCRIPTIONPLAN_BILLINGFREQUENCY_DICT = dict(zip(df_subscription_plan_id['SubscriptionPlanId'].tolist(), df_subscription_plan_id['SubscriptionPlanBillingFrequency'].tolist()))
del df_subscription_plan_id_2

def extract_country(subscription_plan_id, channel, title, country_list):
  """
  Extract country of subscription_plan_id from a raw (json) dictionary.
  """
  
  if channel == 'vg' or channel == 'hor':
    return 'NL'
  
  for country in country_list:
    if country.lower() in title.lower():
      return country.upper()
    elif country.lower() in subscription_plan_id.lower():
      return country.upper() 
    
    if subscription_plan_id == '0-11-lov-month-plan':
      return 'NL'
    elif subscription_plan_id == '0-11-svodsubscriptionplan':
      return 'NL'
  return None

country_list = ['NL', 'BE', 'GB', 'IE', 'UK', 'NZ', 'AU', 'SE', 'NO']

df_subscription_plan_id['country'] = df_subscription_plan_id[['SubscriptionPlanId','channel','title']] \
                                    .apply(lambda x: extract_country(x[0],x[1],x[2],country_list), axis =1)
del df_subscription_plan_id['title']

# COMMAND ----------

def set_payment_event(**kwargs):
  empty_event = {'amount_value': None, 'subscription_id': None}
  kwargs = {**empty_event, **kwargs}
  return {
  "body": {
    "amount": {"value": kwargs['amount_value']},
    "merchant_reference": kwargs['subscription_id'] #=subscription_id
    }
  }

def draw_sample_subscription_plan_id():
  """
  Random sample from the existing subscription_plan_ids. 
  The current approach is to first sample from platform 50-50 (narrowing down the # of subscription_plan_ids), then sample from the countries in the platform, and then sample the contract type/billing frequency with some probability.
  """
  channel_list = ['nfn','lov']
  billing_frequency_list = [30, 365]
  country_lov_list = ['NL', 'NO', 'SE', 'BE']
  country_nfn_list = ['NL', 'NO', 'SE', 'AU', 'NZ', 'GB', 'IE']
  channel = random.choices(channel_list, weights=[0.5, 0.5])[0]
  billing_frequency = random.choices(billing_frequency_list, weights=[0.8, 0.2])[0]
  
  if channel == 'nfn':
    country = random.choices(country_nfn_list, weights=[0.5, 0.1, 0.1, 0.1, 0.1, 0.08, 0.02])[0]
  elif channel == 'lov':
    country = random.choices(country_lov_list, weights=[0.5, 0.24, 0.24, 0.02])[0]
    
  id_df = df_subscription_plan_id[(df_subscription_plan_id['channel'] == channel) \
                                  & (df_subscription_plan_id['country'] == country) \
                                  & (df_subscription_plan_id['SubscriptionPlanBillingFrequency'] == billing_frequency)]

  return random.sample(id_df['SubscriptionPlanId'].unique().tolist(), 1)[0]

def set_subscription_event(*args):
  """
  Basis for the subscription event. If the JSON in the args does not contain one of the fields described below, default_event will take the value present in this function.
  """
  kwargs = args[0]
  default_event = {
   'Date': None,
   'Channel': None,
   'CustomerRegistrationCountry': None,
   'SubscriptionId': None,
   'CustomerId': None,
   'SubscriptionCreatedDate': None,
   'SubscriptionStartDate': None,
   'SubscriptionEndDate': None,
   'RecurringEnabled': True,
   'SubscriptionFreeTrial': 14,
   'SubscriptionImpliedFreeTrial': 14,
   'SubscriptionPlanId': None,
   'SubscriptionPlanBillingCycleType': None,
   'SubscriptionPlanBillingFrequency': None,
   'SubscriptionPrice': None,
   'SubscriptionCurrency': None,
   'SubscriptionPlanFreeTrial': None,
   'AdyenTrialDisputes': 0.0,
   'AdyenDisputes': 0.0,
   'AdyenDisputeTotalAmount': 0.0,
   'AdyenDisputeFirstDispute': None,
   'AdyenDisputeVoluntaryChargebacks': 0.0,
   'AdyenDisputeInvoluntaryChargebacks': 0.0,
   'AdyenTrialPayments': 0.0,
   'AdyenPayments': 0.0,
   'AdyenPaymentTotalAmount': 0.0,
   'AdyenPaymentTotalAmountInSettlementCurrency': 0.0,
   'AdyenPaymentAverageCcyFxRate': None,
   'AdyenPaymentFirstPayment': None,
   'AdyenPaymentFirstPaymentReceived': None,
   'AdyenPaymentFirstPaymentAuthorised': None,
   'AdyenPaymentFirstPaymentSentForSettled':None,
   'AdyenPaymentFirstPaymentSettled': None,
   'AdyenPaymentLastRecurringPaymentMethod': '',
   'AdyenPaymentSumCommission': 0.0,
   'AdyenPaymentSumMarkup': None,
   'AdyenPaymentSumSchemeFee': None,
   'AdyenPaymentSumInterchange': None,
   'SubscriptionIdentifier': None,
   'SubscriptionState': 'activated',
   'SubscriptionNewPlanId': ''
  }
  event = {**default_event, **kwargs}
  return event

def create_new_trialist(df_subscription_plan_id, date = datetime.now().date()):
  """
  Create a JSON for new trialists, random sampling a subscription plan id, and filling all other fields accordingly.
  """
  sampled_subscription_plan_id = draw_sample_subscription_plan_id()
  df_subscription_plan_id_row = df_subscription_plan_id[df_subscription_plan_id['SubscriptionPlanId'] == sampled_subscription_plan_id]
  
  event_input = {}
  event_input['Date'] = date
  event_input['Channel'] = CHANNEL_DICT[df_subscription_plan_id_row['channel'].iloc[0]]
  event_input['SubscriptionId'] = str(uuid.uuid4())
  event_input['CustomerId'] = str(uuid.uuid4())
  event_input['CustomerRegistrationCountry'] = df_subscription_plan_id_row['country'].iloc[0]
  event_input['SubscriptionCreatedDate'] = date
  event_input['SubscriptionStartDate'] = date
  event_input['SubscriptionEndDate'] = date + timedelta(days = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0])
  event_input['AdyenTrialPayments'] = 1
  event_input['AdyenPayments'] = 0
  event_input['AdyenPaymentTotalAmount'] = 0.02
  event_input['AdyenPaymentAverageCcyFxRate'] = CCY_TO_RATE_DICT[df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]]
  event_input['AdyenPaymentTotalAmountInSettlementCurrency'] = 0.02 / CCY_TO_RATE_DICT[df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]]
  event_input['SubscriptionPlanId'] = df_subscription_plan_id_row['SubscriptionPlanId'].iloc[0]
  event_input['SubscriptionPlanBillingCycleType'] = df_subscription_plan_id_row['SubscriptionPlanBillingCycleType'].iloc[0]
  event_input['SubscriptionPlanBillingFrequency'] = df_subscription_plan_id_row['SubscriptionPlanBillingFrequency'].iloc[0]
  event_input['SubscriptionPlanFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0]
  event_input['SubscriptionPrice'] = df_subscription_plan_id_row['SubscriptionPrice'].iloc[0]
  event_input['SubscriptionCurrency'] = df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]
  event_input['SubscriptionFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0]
  event_input['AdyenPaymentSumCommission'] = 0.25
  event_input['AdyenPaymentFirstPayment'] = date
  event_input['AdyenPaymentFirstPaymentReceived'] = date
  event_input['AdyenPaymentFirstPaymentAuthorised'] = date
  event_input['AdyenPaymentFirstPaymentSentForSettled'] = date
  event_input['AdyenPaymentFirstPaymentSettled'] = date
  return set_subscription_event(event_input)

def create_new_voucher(df_subscription_plan_id, date = datetime.now().date()):
  """
  Create a JSON for new voucher users, random sampling a subscription plan id, and filling all other fields accordingly.
  """
  sampled_subscription_plan_id = draw_sample_subscription_plan_id()
  df_subscription_plan_id_row = df_subscription_plan_id[df_subscription_plan_id['SubscriptionPlanId'] == sampled_subscription_plan_id]
  
  event_input = {}
  event_input['Date'] = date
  event_input['Channel'] = CHANNEL_DICT[df_subscription_plan_id_row['channel'].iloc[0]]
  event_input['SubscriptionId'] = str(uuid.uuid4())
  event_input['CustomerId'] = str(uuid.uuid4())
  event_input['CustomerRegistrationCountry'] = df_subscription_plan_id_row['country'].iloc[0]
  event_input['SubscriptionCreatedDate'] = date
  event_input['SubscriptionStartDate'] = date
  event_input['SubscriptionEndDate'] = date + timedelta(days = SUBSCRIPTIONPLAN_BILLINGFREQUENCY_DICT[sampled_subscription_plan_id])
  event_input['AdyenTrialPayments'] = 1
  event_input['AdyenPayments'] = 0
  event_input['AdyenPaymentTotalAmount'] = 0.02
  event_input['AdyenPaymentAverageCcyFxRate'] = CCY_TO_RATE_DICT[df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]]
  event_input['AdyenPaymentTotalAmountInSettlementCurrency'] = 0.02 / CCY_TO_RATE_DICT[df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]]
  event_input['SubscriptionPlanId'] = df_subscription_plan_id_row['SubscriptionPlanId'].iloc[0]
  event_input['SubscriptionPlanBillingCycleType'] = df_subscription_plan_id_row['SubscriptionPlanBillingCycleType'].iloc[0]
  event_input['SubscriptionPlanBillingFrequency'] = df_subscription_plan_id_row['SubscriptionPlanBillingFrequency'].iloc[0]
  event_input['SubscriptionPlanFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0]
  event_input['SubscriptionPrice'] = df_subscription_plan_id_row['SubscriptionPrice'].iloc[0]
  event_input['SubscriptionCurrency'] = df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]
  event_input['SubscriptionFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanBillingFrequency'].iloc[0]
  event_input['AdyenPaymentSumCommission'] = 0.25
  event_input['AdyenPaymentFirstPayment'] = date
  event_input['AdyenPaymentFirstPaymentReceived'] = date
  event_input['AdyenPaymentFirstPaymentAuthorised'] = date
  event_input['AdyenPaymentFirstPaymentSentForSettled'] = date
  event_input['AdyenPaymentFirstPaymentSettled'] = date
  event_input['RecurringEnabled'] = False
  return set_subscription_event(event_input)

def basic_to_premium_switch(date, subscription_event, debug = False):
  """
  Principle:
  - Change subscription_plan_id
  - Shift end_date based on the remaining number of days left for the basic subscription.
  - Change subscription_plan details based on cosmos data
  """
  try:
    new_subscription_plan_id = BASIC_TO_PREMIUM_MAPPER[subscription_event['SubscriptionPlanId']]
  except:
    #if this fails, the subscription plan is a half year, which there will exist no premium for
    return subscription_event

  df_subscription_plan_id_row = df_subscription_plan_id[df_subscription_plan_id['SubscriptionPlanId'] == new_subscription_plan_id]
  days_before_end = subscription_event['SubscriptionEndDate'] - date
  days_before_end = days_before_end.days
  if days_before_end > 1:
    money_left = (days_before_end / subscription_event['SubscriptionPlanBillingFrequency']) * subscription_event['SubscriptionPrice']
  else:
    money_left = 0
  new_price_per_day = df_subscription_plan_id_row['SubscriptionPrice'].iloc[0] / df_subscription_plan_id_row['SubscriptionPlanBillingFrequency'].iloc[0]
  new_days_left = int(math.ceil(money_left / new_price_per_day))
  new_subscription_event = {}
  try:
    new_subscription_event['Date'] = date
    new_subscription_event['Channel'] = CHANNEL_DICT[df_subscription_plan_id_row['channel'].iloc[0]]
    new_subscription_event['SubscriptionId'] = str(uuid.uuid4())
    new_subscription_event['CustomerId'] = subscription_event['CustomerId'] 
    new_subscription_event['CustomerRegistrationCountry'] = subscription_event['CustomerRegistrationCountry']
    new_subscription_event['SubscriptionCreatedDate'] = date
    new_subscription_event['SubscriptionStartDate'] = date
    new_subscription_event['SubscriptionPlanId'] =  new_subscription_plan_id
    new_subscription_event['SubscriptionEndDate'] = subscription_event['SubscriptionEndDate'] - timedelta(days = days_before_end - new_days_left)
    new_subscription_event['SubscriptionPlanBillingCycleType'] = df_subscription_plan_id_row['SubscriptionPlanBillingCycleType'].iloc[0]
    new_subscription_event['SubscriptionPlanBillingFrequency'] = df_subscription_plan_id_row['SubscriptionPlanBillingFrequency'].iloc[0]
    new_subscription_event['SubscriptionPrice'] = df_subscription_plan_id_row['SubscriptionPrice'].iloc[0]
    new_subscription_event['SubscriptionCurrency'] = df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]
    new_subscription_event['SubscriptionPlanFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0]  
    new_subscription_event['SubscriptionImpliedFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0]
    new_subscription_event['SubscriptionFreeTrial'] = 0
    new_subscription_event['AdyenPayments'] = 0
    new_subscription_event['AdyenPaymentAverageCcyFxRate'] = CCY_TO_RATE_DICT[df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]]
    new_subscription_event['AdyenPaymentTotalAmountInSettlementCurrency'] = 0
  except Exception as e:
    print(new_subscription_plan_id)
    print(e)
  if debug:
    print("UserId {} switched from Basic to Premium!".format(new_subscription_event['CustomerId']))
  return set_subscription_event(new_subscription_event)

def premium_to_basic_switch(date, subscription_event, debug = False):
  """
  Principle:
  - Assumption: This method is called on the end date of the previous premium subscription plan; and this one is paid immediately
  - Change subscription_plan_id
  - Change subscription_plan details based on cosmos data
  """
  new_subscription_plan_id = subscription_event['SubscriptionNewPlanId']
  df_subscription_plan_id_row = df_subscription_plan_id[df_subscription_plan_id['SubscriptionPlanId'] == new_subscription_plan_id]

  new_subscription_event = {}
  new_subscription_event['Date'] = date
  new_subscription_event['Channel'] = CHANNEL_DICT[df_subscription_plan_id_row['channel'].iloc[0]]
  new_subscription_event['SubscriptionId'] = str(uuid.uuid4())
  new_subscription_event['CustomerId'] = subscription_event['CustomerId'] 
  new_subscription_event['CustomerRegistrationCountry'] = subscription_event['CustomerRegistrationCountry']
  new_subscription_event['SubscriptionCreatedDate'] = date
  new_subscription_event['SubscriptionStartDate'] = date
  new_subscription_event['SubscriptionPlanId'] =  new_subscription_plan_id
  new_subscription_event['SubscriptionEndDate'] = subscription_event['SubscriptionEndDate'] + timedelta(days = SUBSCRIPTIONPLAN_BILLINGFREQUENCY_DICT[new_subscription_plan_id])
  new_subscription_event['SubscriptionPlanBillingCycleType'] = df_subscription_plan_id_row['SubscriptionPlanBillingCycleType'].iloc[0]
  new_subscription_event['SubscriptionPlanBillingFrequency'] = df_subscription_plan_id_row['SubscriptionPlanBillingFrequency'].iloc[0]
  new_subscription_event['SubscriptionPrice'] = df_subscription_plan_id_row['SubscriptionPrice'].iloc[0]
  new_subscription_event['SubscriptionCurrency'] = df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]
  new_subscription_event['SubscriptionPlanFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0]  
  new_subscription_event['SubscriptionImpliedFreeTrial'] = df_subscription_plan_id_row['SubscriptionPlanFreeTrial'].iloc[0]
  new_subscription_event['SubscriptionFreeTrial'] = 0
  new_subscription_event['AdyenPayments'] = 1
  new_subscription_event['AdyenPaymentAverageCcyFxRate'] = CCY_TO_RATE_DICT[df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]]
  new_subscription_event['AdyenPaymentTotalAmountInSettlementCurrency'] = PRICEPLAN_DICT[new_subscription_plan_id] * CCY_TO_RATE_DICT[df_subscription_plan_id_row['SubscriptionCurrency'].iloc[0]]
  new_subscription_event['AdyenPaymentTotalAmount'] = PRICEPLAN_DICT[new_subscription_plan_id]
  new_subscription_event['AdyenPaymentFirstPayment'] = date
  new_subscription_event['AdyenPaymentFirstPaymentReceived'] = date
  new_subscription_event['AdyenPaymentFirstPaymentAuthorised'] = date
  new_subscription_event['AdyenPaymentFirstPaymentSentForSettled'] = date
  new_subscription_event['AdyenPaymentFirstPaymentSettled'] = date
  
  if debug:
    print("UserId {} switched from Premium to basic!".format(new_subscription_event['CustomerId']))
    
  return set_subscription_event(new_subscription_event)

# COMMAND ----------

def simulate_record(options, for_date, record, debug = False):
  # If RecurringEnabled is false, then this subscription is cancelled, state is fixed
  if (not record['RecurringEnabled']):
    return record
  
  # The odds of churning
  if (random.random() < options['churnRatePerDay']):
    record['RecurringEnabled'] = False
    return record
  
  # If for_date is same as SubscriptionEndDate, then add a new payment to the current state
  if (record['SubscriptionEndDate'] == for_date):
    record['SubscriptionStartDate'] = for_date
    record['SubscriptionEndDate'] = for_date + timedelta(days = SUBSCRIPTIONPLAN_BILLINGFREQUENCY_DICT[record['SubscriptionPlanId']])
    record['AdyenPayments'] = record['AdyenPayments'] + 1
    record['AdyenPaymentTotalAmountInSettlementCurrency'] = record['AdyenPaymentTotalAmountInSettlementCurrency'] + PRICEPLAN_DICT[record['SubscriptionPlanId']] * CCY_TO_RATE_DICT[record['SubscriptionCurrency']]
    record['AdyenPaymentTotalAmount'] = record['AdyenPaymentTotalAmount'] + PRICEPLAN_DICT[record['SubscriptionPlanId']]
    if (record['AdyenPayments'] == 1):
      record['AdyenPaymentFirstPayment'] = for_date
      record['AdyenPaymentFirstPaymentReceived'] = for_date
      record['AdyenPaymentFirstPaymentAuthorised'] = for_date
      record['AdyenPaymentFirstPaymentSentForSettled'] = for_date
      record['AdyenPaymentFirstPaymentSettled'] = for_date
    return record
  
  # If in trial period, then no switching
  if (record['SubscriptionCreatedDate'] + timedelta(days = 0 if math.isnan(record['SubscriptionFreeTrial']) else record['SubscriptionFreeTrial']) > for_date):
    return record
  
  # The odds of switching from basic to premium
  if (record['SubscriptionPlanId'] in list(BASIC_TO_PREMIUM_MAPPER.keys()) and random.random() < options['basicToPremiumRatePerDay']):
    record['SubscriptionNewPlanId'] = BASIC_TO_PREMIUM_MAPPER[record['SubscriptionPlanId']]
    record['RecurringEnabled'] = False
    return record
  
  # The odds of switching from premium to basic
  if (record['SubscriptionPlanId'] in list(PREMIUM_TO_BASIC_MAPPER.keys()) and random.random() < options['premiumToBasicRatePerDay']):
    record['SubscriptionNewPlanId'] = PREMIUM_TO_BASIC_MAPPER[record['SubscriptionPlanId']]
    record['RecurringEnabled'] = False
    return record
  
  # If nothing noteworty happened, return current state
  return record

def simulate_clear_switch(options, for_date, record):
  if (record['SubscriptionPlanId'] in list(BASIC_TO_PREMIUM_MAPPER.keys()) and record['SubscriptionNewPlanId'] in list(BASIC_TO_PREMIUM_MAPPER.values())):
    record['SubscriptionEndDate'] = for_date + timedelta(days = -1)
    record['SubscriptionNewPlanId'] = ''
    record['RecurringEnabled'] = False
  
  if (record['SubscriptionPlanId'] in list(PREMIUM_TO_BASIC_MAPPER.keys()) and record['SubscriptionNewPlanId'] in list(PREMIUM_TO_BASIC_MAPPER.values()) and record['SubscriptionEndDate'] == for_date):
    record['SubscriptionEndDate'] = for_date + timedelta(days = -1)
    record['SubscriptionNewPlanId'] = ''
    record['RecurringEnabled'] = False
  return record

# COMMAND ----------

df_sim = df.copy()
sample = random.sample(df_sim.index.tolist(), 10)
df_sim = df_sim[df_sim.index.isin(sample)]

# COMMAND ----------

#TODO: Check whether the premium to basic suffices -> Waiting for Axinom
#TODO: Check what to do with trialists (Now we assume subscribers can't switch in trial period) -> Waiting for Milan & Axinom

for day_nr in range(0, SIMULATION_OPTIONS['days']):
  date = datetime.now().date() + timedelta(days = day_nr)
  print("Starting day {}.".format(date))
  
  # Apply linear state transformations
  df_sim = df_sim.apply(lambda x: simulate_record(SIMULATION_OPTIONS, date, x, debug=DEBUG_MODE), axis=1, result_type='broadcast')
  
  # Handle the switching to premium
  df_new_premiums = df_sim[
      df_sim['SubscriptionPlanId'].isin(list(BASIC_TO_PREMIUM_MAPPER.keys()))
      & df_sim['SubscriptionNewPlanId'].isin(list(BASIC_TO_PREMIUM_MAPPER.values()))
    ].apply(lambda x: basic_to_premium_switch(date, x, debug=DEBUG_MODE), axis=1, result_type='expand')
  
  # Handle the switching to basic
  df_new_basics = df_sim[
      df_sim['SubscriptionPlanId'].isin(list(PREMIUM_TO_BASIC_MAPPER.keys()))
      & df_sim['SubscriptionNewPlanId'].isin(list(PREMIUM_TO_BASIC_MAPPER.values()))
      & (df_sim['SubscriptionEndDate'] == date)
    ].apply(lambda x: premium_to_basic_switch(date, x, debug=DEBUG_MODE), axis=1, result_type='expand')
  
  # Clean up
  df_sim = df_sim.apply(lambda x: simulate_clear_switch(SIMULATION_OPTIONS, date, x), axis=1, result_type='broadcast')
  
  # Add new trialists
  new_trialists = poisson.rvs(mu=SIMULATION_OPTIONS['meanNewTrialsPerDay'])
  df_new_trials = []
  for i in range(0, new_trialists):
    df_new_trials.append(create_new_trialist(df_subscription_plan_id, date = date))
    
  # Add new vouchers
  new_vouchers = poisson.rvs(mu=SIMULATION_OPTIONS['meanNewVouchersPerDay'])
  df_new_vouchers = []
  for i in range(0, new_vouchers):
    df_new_vouchers.append(create_new_voucher(df_subscription_plan_id, date = date))
  
  # Merge all dataframes
  df_sim = df_sim.reset_index()
  df_sim = df_sim.append(df_new_premiums, ignore_index=True)
  df_sim = df_sim.append(df_new_basics, ignore_index=True)
  df_sim = df_sim.append(df_new_trials, ignore_index=True)
  df_sim = df_sim.append(df_new_vouchers, ignore_index=True)
  df_sim = df_sim.set_index('SubscriptionId')
  
  # Print summary
  print("Total subscription records: {}.".format(df_sim.shape[0]))
  print("Active records: {}.".format((df_sim['RecurringEnabled'] | (df_sim['SubscriptionNewPlanId'] != '')).sum()))
  print("New trials: {}.".format(new_trialists))
  print("New switch to premium: {}.".format(df_new_premiums.shape[0]))
  print("New switch to basic: {}.".format(df_new_basics.shape[0]))

# COMMAND ----------

"""
WRITE RESULTS TO ADX - UNFINISHED.
"""
def to_datetime(x):
  try:
    return datetime(x.year, x.month, x.day, 0, 0, 0)
  except Exception as e:
    return None

cols =["Date", "Channel", 'CustomerId', 'CustomerRegistrationCountry', 'SubscriptionId', 'SubscriptionCreatedDate', 'SubscriptionStartDate', 'SubscriptionEndDate', 'RecurringEnabled', 'SubscriptionFreeTrial', 'SubscriptionImpliedFreeTrial', 'SubscriptionIdentifier', 'SubscriptionState', 'SubscriptionNewPlanId', 'SubscriptionPlanId', 'SubscriptionPlanBillingCycleType', 'SubscriptionPlanBillingFrequency', 'SubscriptionPrice', 'SubscriptionCurrency', 'SubscriptionPlanFreeTrial', 'AdyenTrialDisputes', 'AdyenDisputes', 'AdyenDisputeTotalAmount', 'AdyenDisputeFirstDispute', 'AdyenDisputeVoluntaryChargebacks', 'AdyenDisputeInvoluntaryChargebacks', 'AdyenTrialPayments', 'AdyenPayments', 'AdyenPaymentTotalAmount', 'AdyenPaymentTotalAmountInSettlementCurrency', 'AdyenPaymentAverageCcyFxRate', 'AdyenPaymentFirstPayment', 'AdyenPaymentFirstPaymentReceived', 'AdyenPaymentFirstPaymentAuthorised', 'AdyenPaymentFirstPaymentSentForSettled', 'AdyenPaymentFirstPaymentSettled', 'AdyenPaymentLastRecurringPaymentMethod', 'AdyenPaymentSumCommission', 'AdyenPaymentSumMarkup', 'AdyenPaymentSumSchemeFee', 'AdyenPaymentSumInterchange']
datecols = ['SubscriptionCreatedDate', "SubscriptionStartDate", "SubscriptionEndDate", 'AdyenPaymentFirstPayment','AdyenPaymentFirstPaymentReceived', 'AdyenPaymentFirstPaymentSettled', 'Date', 'AdyenDisputeFirstDispute', 'AdyenPaymentFirstPaymentAuthorised', 'AdyenPaymentFirstPaymentSentForSettled']

df_to_adx = df_sim.copy().reset_index()

for col in datecols:
  df_to_adx[col] = df_to_adx[col].apply(lambda x: to_datetime(x))  

df_to_adx = df_to_adx[cols]
schema = StructType([StructField("Date", TimestampType(), False),
                     StructField("Channel", StringType(), False),
                     StructField("CustomerId", StringType(), False),
                     StructField("CustomerRegistrationCountry", StringType(), True),
                     StructField("SubscriptionId", StringType(), False),
                     StructField("SubscriptionCreatedDate", TimestampType(), True),
                     StructField("SubscriptionStartDate", TimestampType(), True),
                     StructField("SubscriptionEndDate", TimestampType(), False),
                     StructField("RecurringEnabled", BooleanType(), True),
                     StructField("SubscriptionFreeTrial", FloatType(), True),
                     StructField("SubscriptionImpliedFreeTrial", FloatType(), True),
                     StructField("SubscriptionIdentifier", StringType(), True),
                     StructField("SubscriptionState", StringType(), True),
                     StructField("SubscriptionNewPlanId", StringType(), True),
                     StructField("SubscriptionPlanId", StringType(), True),
                     StructField("SubscriptionPlanBillingCycleType", StringType(), True),
                     StructField("SubscriptionPlanBillingFrequency", IntegerType(), True),
                     StructField("SubscriptionPrice", FloatType(), True),
                     StructField("SubscriptionCurrency", StringType(), True),
                     StructField("SubscriptionPlanFreeTrial", FloatType(), True),
                     StructField("AdyenTrialDisputes", IntegerType(), True),
                     StructField("AdyenDisputes", IntegerType(), True),
                     StructField("AdyenDisputeTotalAmount", FloatType(), True),
                     StructField("AdyenDisputeFirstDispute", TimestampType(), True),
                     StructField("AdyenDisputeVoluntaryChargebacks", IntegerType(), True),
                     StructField("AdyenDisputeInvoluntaryChargebacks", IntegerType(), True),
                     StructField("AdyenTrialPayments", IntegerType(), True),
                     StructField("AdyenPayments", IntegerType(), True),
                     StructField("AdyenPaymentTotalAmount", FloatType(), True),
                     StructField("AdyenPaymentTotalAmountInSettlementCurrency", FloatType(), True),
                     StructField("AdyenPaymentAverageCcyFxRate", FloatType(), True),
                     StructField("AdyenPaymentFirstPayment", TimestampType(), True),
                     StructField("AdyenPaymentFirstPaymentReceived", TimestampType(), True),
                     StructField("AdyenPaymentFirstPaymentAuthorised", TimestampType(), True),
                     StructField("AdyenPaymentFirstPaymentSentForSettled", TimestampType(), True),
                     StructField("AdyenPaymentFirstPaymentSettled", TimestampType(), True),
                     StructField("AdyenPaymentLastRecurringPaymentMethod", StringType(), True),
                     StructField("AdyenPaymentSumCommission", FloatType(), True),
                     StructField("AdyenPaymentSumMarkup", FloatType(), True),
                     StructField("AdyenPaymentSumSchemeFee", FloatType(), True),
                     StructField("AdyenPaymentSumInterchange", FloatType(), True),
                    ])

dfs = spark.createDataFrame(df_to_adx,schema)
adx.append(dfs, "20210824_subscriptions_simulation", authentication_dict, kustoDatabase = 'playground', write = True)

# COMMAND ----------


