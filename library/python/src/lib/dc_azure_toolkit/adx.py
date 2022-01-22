import sys

from pyspark.sql import SparkSession

def select(query, authentication_dict, cluster = 'uelprdadx', kustoDatabase = 'uel', df_format = 'dfs', spark = None):
    """
    Authenticates with a given Azure Data Explorer (ADX) cluster, and retrieves data from the given kusto query.

    :param query: adx query to run on the target database
    :param authentication_dict: dictionary with adx credentials, should contain four keys: cluster_id, client_id, client_secret and authority_id.
    :param kustoDatabase: the database in which to query on the selected resource_group
    :param df_format: description of the return type, dfs for lightweighted spark dataframes, df for more heavy weighted pandas dataframes.
    :param spark: sparkSession object

    :type query: str
    :type authentication_dict: dict
    :type kustoDatabase: str
    :type df_format: str
    :type spark: pyspark.sql.session.SparkSession
    """

    supported_df_format = ['dfs', 'df']
    if df_format not in supported_df_format:
        sys.exit("Dataframe format not supported!")

    if isinstance(spark, type(None)):
        spark = SparkSession.builder.appName("kustoPySpark").getOrCreate()

    #Unpack authentication details:
    cluster_id = authentication_dict['cluster']
    client_id = authentication_dict['client_id']
    client_secret = authentication_dict['client_secret']
    authority_id = authentication_dict['authority_id']

    df = spark.read. \
        format("com.microsoft.kusto.spark.datasource"). \
        option("kustoCluster", cluster_id). \
        option("kustoDatabase", kustoDatabase). \
        option("kustoQuery", query). \
        option("kustoAadAppId", client_id). \
        option("kustoAadAppSecret", client_secret). \
        option("kustoAadAuthorityID", authority_id). \
        load()

    if df_format == 'df':
        df = df.toPandas()

    return df
  
def append(dfs, destination_table_name, authentication_dict, kustoDatabase = 'uel', write = True):
    """
    Write a spark dataframe to a selected data explorer table. View the microsoft documentation on: https://docs.microsoft.com/en-us/azure/data-explorer/spark-connector.

    :param dfs: spark dataframe
    :param destination_table_name: name of the table of the target destination
    :param authentication_dict: dictionary with adx credentials
    :param kustoDatabase: the database in which to query on the selected resource_group
    :param write: whether to actually proceed writing to the target destination

    :type dfs: spark dataframe
    :type destination_table_name: str
    :type authentication_dict: str
    :type kustoDatabase: str
    :type write: boolean
    """
    #Unpack authentication details:
    cluster_id = authentication_dict['cluster']
    client_id = authentication_dict['client_id']
    client_secret = authentication_dict['client_secret']
    authority_id = authentication_dict['authority_id']

    if write:
        dfs.write. \
          format("com.microsoft.kusto.spark.datasource"). \
          option("kustoCluster", cluster_id). \
          option("kustoDatabase", kustoDatabase). \
          option("kustoTable", destination_table_name). \
          option("kustoAadAppId", client_id). \
          option("kustoAadAppSecret", client_secret). \
          option("kustoAadAuthorityID", authority_id). \
          option("tableCreateOptions","CreateIfNotExist"). \
          mode("append"). \
          save()