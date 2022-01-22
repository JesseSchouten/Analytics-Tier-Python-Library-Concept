# Databricks notebook source
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

class ADXAuthenticator():
  """
  Gets all parameters needed to authenticate with known data explorer resources. Can be used to retrieve data from data explorer.
  
  :param resource_name: name of the data explorer resource, for example uelprdadx
  :type resource_name: str
  """
  def __init__(self, resource_name):    
    supported_resources = ['uelprdadx', 'ueldevadx']
  
    if resource_name not in supported_resources:
      sys.exit("ERROR - Enter a valid azure ADX resource!")
      
    if resource_name == 'uelprdadx':
      self.cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
      self.client_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-clientid")
      self.client_secret = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-clientsecret")
      self.authority_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-authorityid")

    elif resource_name == 'ueldevadx':
      self.cluster = "https://ueldevadx.westeurope.kusto.windows.net" # id 
      self.client_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-dev-adx-clientid")
      self.client_secret = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-dev-adx-clientsecret")
      self.authority_id = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-dev-adx-authorityid")
      
  def get_authentication_dict(self, db = None):
    return {
      'cluster': self.cluster,
      'client_id': self.client_id,
      'client_secret': self.client_secret,
      'authority_id': self.authority_id
    }

# COMMAND ----------

def get_authentication_dict_cosmosdb(resource_name):
  """
  Get the dictionary with required authentication credentials for connection to a given cosmos database instance.
  
  :param resource_name: name of the cosmosDB resource.
  :type resource_name: str
  """
  if resource_name not in ['uds-prd-db-account']:
    sys.exit("ERROR - Enter a valid azure Cosmos server!")
  if resource_name == 'uds-prd-db-account':
    result = {
              'account_url' : 'https://uds-prd-db-account.documents.azure.com:443/',
              'account_key': dbutils.secrets.get(scope = "data-prd-keyvault", key = "uds-prd-db-account-accountkey")
             }
  return result

class CosmosAuthenticator():
  """
  Get the dictionary with required authentication credentials for connection to a given cosmos database instance.
  
  :param resource_name: name of the cosmosDB resource.
  :type resource_name: str
  """
  def __init__(self, resource_name):    
    supported_resources = ['uds-prd-db-account']
  
    if resource_name not in supported_resources:
      sys.exit("ERROR - Enter a valid azure Cosmos server!")
      
    if resource_name == 'uds-prd-db-account':     
      self.account_url = 'https://uds-prd-db-account.documents.azure.com:443/'
      self.account_key = dbutils.secrets.get(scope = "data-prd-keyvault", key = "uds-prd-db-account-accountkey")
      
  def get_authentication_dict(self, db = None):
    return {
        'account_url' : self.account_url,
        'account_key': self.account_key
    }

# COMMAND ----------

def get_storage_acccount_authentication_dict(storage_account_name):
  """
  Gets the account key of the a storage account from the azure keyvault.
  
  :param storage_account_name: name of the storage account, e.g. dcinternal.
  :type storage_account_name: str
  """
  
  supported_resources = ['dcinternal', 'udsdevsuppliersa','udsprdsuppliersa']
  
  if storage_account_name not in supported_resources:
    sys.exit("ERROR - Enter a valid azure storage account!")

  if storage_account_name == 'dcinternal':
    storage_account_key = dbutils.secrets.get(scope = "data-prd-keyvault", key = "dcinternal-account-key")
  elif storage_account_name == 'udsdevsuppliersa':
    self.storage_account_key = dbutils.secrets.get(scope = "data-prd-keyvault", key = "udsdevsuppliersa-account-key")
    
  return {
    'storage_account_key': storage_account_key
  }

class StorageAccountAuthenticator():
  """
  Gets dictionary with authentication credentials for a storage account from the azure keyvault.
  
  :param storage_account_name: name of the storage account, e.g. dcinternal.
  :type storage_account_name: str
  """
  
  def __init__(self, storage_account_name):
    supported_resources = ['dcinternal', 'udsdevsuppliersa','udsprdsuppliersa']
  
    if storage_account_name not in supported_resources:
      sys.exit("ERROR - Enter a valid azure storage account!")
    
    if storage_account_name == 'dcinternal':
      self.storage_account_key = dbutils.secrets.get(scope = "data-prd-keyvault", key = "dcinternal-account-key")
    elif storage_account_name == 'udsdevsuppliersa':
      self.storage_account_key = dbutils.secrets.get(scope = "data-prd-keyvault", key = "udsdevsuppliersa-account-key")
    elif storage_account_name == 'udsprdsuppliersa':
      self.storage_account_key = dbutils.secrets.get(scope = "data-prd-keyvault", key = "udsprdsuppliersa-account-key-1")
      
  def get_authentication_dict(self):
    return {
      'storage_account_key': self.storage_account_key
    }
