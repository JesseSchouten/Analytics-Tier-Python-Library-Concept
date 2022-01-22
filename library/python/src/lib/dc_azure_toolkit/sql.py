import sys
from urllib.parse import urlparse

import pyodbc
import pandas as pd
from pyspark.sql import SparkSession

def select_table(table_name, authentication_dict, df_format='dfs', spark = None):
    """
    Returns data from a table in a SQL server database. It returns the data as a spark dataframe by default, and can be used for relatively heavy workloads.

    :param table_name: name of the table from which to retrieve the data.
    :param authentication_dict: dictionary with adx credentials, should contain four keys: cluster_id, client_id, client_secret and authority_id.
    :param df_format: description of the return type, dfs for lightweighted spark dataframes, df for more heavy weighted pandas dataframes.

    :type table_name: str
    :type authentication_dict: dict
    :type df_format: str
    """

    supported_df_format = ['dfs', 'df']
    if df_format not in supported_df_format:
        sys.exit("Dataframe format not supported!")

    if isinstance(spark, type(None)):
        spark = SparkSession.builder.appName('MSSQL').getOrCreate()

    server_name = authentication_dict['server_name']
    database = authentication_dict['database']
    user_name = authentication_dict['username']
    password = urlparse(authentication_dict['password'])

    jdbcUrl = f"jdbc:sqlserver://{server_name};databaseName={database};user={user_name};password={password};encrypt=true;hostNameInCertificate={server_name};"

    df = spark.read.format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", table_name) \
        .option("user", authentication_dict['username']) \
        .option("password", authentication_dict['password']) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    if df_format == 'df':
        df = df.toPandas()

    return df


def select(query, authentication_dict, df_format='df'):
    """
    Returns queried data from a SQL server database. It returns the data as a pandas dataframe by default, and can be used for relatively heavy workloads.

    :param query: sql query from which to retrieve the data.
    :param authentication_dict: dictionary with adx credentials, should contain four keys: cluster_id, client_id, client_secret and authority_id.
    :param df_format: description of the return type, dfs for lightweighted spark dataframes, df for more heavy weighted pandas dataframes.

    :type query: str
    :type authentication_dict: dict
    :type df_format: str
    """

    supported_df_format = ['df']
    if df_format not in supported_df_format:
        sys.exit("Dataframe format not supported!")

    server_name = authentication_dict['server_name']
    database = authentication_dict['database']
    user_name = authentication_dict['username']
    password = authentication_dict['password'].replace("}","}}").replace("{","{{")

    driver = 'ODBC Driver 17 for SQL Server'

    # if ';' in password:
    odbc_driver = "DRIVER={{{}}};SERVER={{{}}};DATABASE={{{}}};UID={{{}}};PWD={{{}}}".format(driver, server_name,
                                                                                             database, user_name,
                                                                                             password)
                                                                                                 
    conn = pyodbc.connect(odbc_driver)

    df = pd.read_sql(query, conn)

    return df