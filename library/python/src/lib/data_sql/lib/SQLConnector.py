import sys
from urllib.parse import urlparse, quote_plus

import pyodbc
import sqlalchemy as db
import pymssql

class SQLConnector:
    """
    Object that manages connections with SQL.

    :param context: context containing at least a SQLAuthenticator object.

    :type context: dict
    """
    def __init__(self, context):
        authenticator = context['SQLAuthenticator']
        self.auth_dict = authenticator.get_authentication_dict()

        driver = 'ODBC Driver 17 for SQL Server'

        self.odbc_driver = "DRIVER={{{}}};SERVER={{{}}};DATABASE={{{}}};UID={{{}}};PWD={{{}}}".format(
                        driver,
                        self.auth_dict['server_name'],
                        self.auth_dict['database'],
                        self.auth_dict['username'],
                        self.auth_dict['password'].replace("}", "}}").replace("{", "{{"))

        self.jdbc_url = f"jdbc:sqlserver://{self.auth_dict['server_name']}" \
                        f";databaseName={self.auth_dict['database']}" \
                        f";user={self.auth_dict['username']}" \
                        f";password={{{self.auth_dict['password'].replace('}', '}}')}}}" \
                        f";encrypt=true" \
                        f";hostNameInCertificate={self.auth_dict['server_name']};"

        self.conn = None

    def open_connection(self, conn_type):
        supported_conn_types = ['odbc', 'jdbc', 'odbc_sqlalchemy', 'pymssql']
        if conn_type not in supported_conn_types:
            sys.exit("ERROR - database connector not supported!")
        if conn_type == 'odbc':
            if pyodbc.drivers() != ['ODBC Driver 17 for SQL Server']:
                sys.exit("ERROR - driver type not present on system!")
            self.conn = pyodbc.connect(self.odbc_driver)
        if conn_type == 'odbc_sqlalchemy':
            if pyodbc.drivers() != ['ODBC Driver 17 for SQL Server']:
                sys.exit("ERROR - driver type not present on system!")
            db_params = quote_plus(self.odbc_driver)
            self.conn = db.create_engine(
                "mssql+pyodbc:///?odbc_connect={}".format(db_params)
            )
        if conn_type == 'pymssql':
            self.conn = pymssql.connect(server=self.auth_dict['server_name']
                                        , user=self.auth_dict['username']
                                        , password=self.auth_dict['password']
                                        , database=self.auth_dict['database']
                                        )
        if conn_type == 'jdbc':
            # do nothing, is handled by library.
            pass

    def close_connection(self):
        self.conn.close()

    def get_status(self):
        if isinstance(self.conn, type(None)):
            print("Connection not yet established")
        else:
            print("Connection closed or open")
