def write_to_csv(dfs, storage_account_name, container_name, authentication_dict, dir_name, file_name, spark=None):
    """
    Writes data as a CSV file to a blob container on a storage account.

    :param storage_account_name: name of the storage account.
    :param authentication_dict: authentication object for the storage account, consisting of at least the account key.
    :param container_name: name of the container.
    :param dir_name: dir name within the container to write the CSV towards.
    :param file_name: file name to give the CSV.
    :param spark: sparkSession object to use.

    :type storage_account_name: str
    :type authentication_dict: dict
    :type container_name: str
    :type dir_name: str
    :type file_name: str
    :type spark: pyspark.sql.session.SparkSession
    """

    if isinstance(dfs, type(None)):
        raise Exception('dfs should not be none')
    if isinstance(storage_account_name, type(None)) and not isinstance(storage_account_name, type(str)):
        raise Exception(
            'storage_account_name should not be none and a str type')
    if isinstance(container_name, type(None)) and not isinstance(container_name, type(str)):
        raise Exception(
            'container_name should not be none and should be str type')
    if isinstance(dir_name, type(None)) and not isinstance(dir_name, type(str)):
        raise Exception(
            'dir_name should not be none and should be str str type')
    if isinstance(file_name, type(None)) and not isinstance(file_name, type(str)):
        raise Exception(
            'file_name should not be none and should be str str type')

    if isinstance(spark, type(None)):
        spark = SparkSession.builder.appName(
            'SparkSession for dc_azure_toolkit - blob.').getOrCreate()

    spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name),
                   authentication_dict['storage_account_key'])

    blob_container = "wasbs://{}@{}.blob.core.windows.net/".format(
        container_name, storage_account_name)
    blob_dir = blob_container + dir_name
    blob_file = blob_dir + file_name
    print('writing file: ' + blob_file)

    try:
        (dfs
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", "true")
         .format("com.databricks.spark.csv")
         .save(blob_dir))
    except:
        print('failed to write file: ' + blob_file)

    # Get the name of the wrangled-data CSV file that was just saved to Azure blob storage (it starts with 'part-')
    files = dbutils.fs.ls(blob_dir)
    output_file = [x for x in files if x.name.startswith("part-")]

    # Move the wrangled-data CSV file from a sub-folder (wrangled_data_folder) to the root of the blob container
    # While simultaneously changing the file name
    dbutils.fs.mv(output_file[0].path, blob_file)
