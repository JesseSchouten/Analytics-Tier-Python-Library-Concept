import sys

from pyspark.sql import SparkSession


def select_from_csv(storage_account_name, container_name, authentication_dict, file_path, delimiter =',', output_format='dfs', spark=None):
    """
    Gets data from a CSV file in a blob container on a storage account.

    :param storage_account_name: name of the storage account.
    :param authentication_dict: authentication object for the storage account, consisting of at least the account key.
    :param container_name: name of the container.
    :param file_path: path within the container to the CSV.
    :param delimiter: delimiter of the csv file.
    :param output_format: format in which to return the data. Currently supported: dfs (Spark DataFrame).
    :param spark: sparkSession object to use.

    :type storage_account_name: str
    :type authentication_dict: dict
    :type container_name: str
    :type file_path: str
    :type delimiter: str
    :type output_format: str
    :type spark: pyspark.sql.session.SparkSession
    """
    if output_format not in ['dfs']:
        sys.exit("Output format not supported!")

    if isinstance(spark, type(None)):
        spark = SparkSession.builder.appName('SparkSession for dc_azure_toolkit - blob.').getOrCreate()

    spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name),
                   authentication_dict['storage_account_key'])

    file = "wasbs://{}@{}.blob.core.windows.net/{}".format(container_name, storage_account_name, file_path)
    dfs = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", delimiter) \
        .csv(file)

    return dfs
