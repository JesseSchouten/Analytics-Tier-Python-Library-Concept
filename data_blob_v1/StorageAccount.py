class StorageAccount():
    def __init__(self, dbutils, key_name, name):
        self.name = name
        self.account_key = dbutils.secrets.get(scope = "data-prd-keyvault", key = key_name)

