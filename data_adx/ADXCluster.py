class ADXClusterUelPrdAdx():
    def __init__(self):
        self.name = 'uelprdadx'
        self.cluster = "https://uelprdadx.westeurope.kusto.windows.net"
        self.client_id = 'dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-clientid")'
        self.client_secret = 'dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-clientsecret")'
        self.authority_id = 'dbutils.secrets.get(scope = "data-prd-keyvault", key = "uel-prd-adx-authorityid")'