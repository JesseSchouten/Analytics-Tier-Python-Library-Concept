class StorageAccountDCInternal():
    def __init__(self):
        self.name = 'dcinternal'
        self.account_key = 'dbutils.secrets.get(scope = "data-prd-keyvault", key = "dcinternal-account-key")'