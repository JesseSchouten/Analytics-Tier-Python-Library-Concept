class SQLAuthenticator:
    """
    Contains all parameters needed to authenticate with a sql server database.

    :param resource_name: name of the resource of the SQL server, e.g. data-prd-sql.
    :param dbutils: databricks utilities object. Are not supported outside the scope of databricks notebooks.
    More info, see https://docs.databricks.com/dev-tools/databricks-utils.html

    :type resource_name: str
    :type dbutils: dbruntime.dbutils.DBUtils
    """

    def __init__(self, resource_name, dbutils=None):
        if resource_name == 'data-prd-sql' and not isinstance(dbutils, type(None)):
            self.password = dbutils.secrets.get(scope="data-prd-keyvault", key="data-prd-sql-datalogin")
            self.server_name = 'data-prd-sql.database.windows.net'
            self.username = 'datalogin'
            self.database = 'dc'
        elif resource_name == 'data-dev-sql' and not isinstance(dbutils, type(None)):
            self.password = dbutils.secrets.get(scope="data-prd-keyvault", key="data-dev-sql-datalogin")
            self.server_name = 'data-dev-sql.database.windows.net'
            self.username = 'datalogin'
            self.database = 'data-dev-sql'
        else:
            self.password = ''
            self.server_name = ''
            self.username = ''
            self.database = ''

    def update_credentials(self, resource_name, dbutils=None):
        if resource_name == 'data-prd-sql' and not isinstance(dbutils, type(None)):
            self.password = dbutils.secrets.get(scope="data-prd-keyvault", key="data-prd-sql-datalogin")
            self.server_name = 'data-prd-sql.database.windows.net'
            self.username = 'datalogin'
            self.database = 'dc'
        elif resource_name == 'data-dev-sql' and not isinstance(dbutils, type(None)):
            self.password = dbutils.secrets.get(scope="data-prd-keyvault", key="data-dev-sql-datalogin")
            self.server_name = 'data-dev-sql.database.windows.net'
            self.username = 'datalogin'
            self.database = 'data-dev-sql'
        else:
            self.password = ''
            self.server_name = ''
            self.username = ''
            self.database = ''

    def get_authentication_dict(self):
        return {
            'server_name': self.server_name,
            'password': self.password,
            'username': self.username,
            'database': self.database
        }
