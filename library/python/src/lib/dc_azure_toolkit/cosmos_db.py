import sys

from pyspark.sql import SparkSession

def select(query, authentication_dict, db, container_name, output_format = 'df', spark = None):
  """
  Authenticates with a given CosmosDB container, and retrieves data from the given query: https://docs.microsoft.com/en-us/azure/cosmos-db/sql/create-sql-api-spark.

  :param query: query to run on the target database.
  :param authentication_dict: dictionary with cosmosDB credentials, should contain two keys: account_url and account_key.
  :param db: the database in which the container is located.
  :param container_name: the container from which to retrieve data on the selected resource.
  :param output_format: description of the return type, dfs for lightweighted spark dataframes, df for more heavy weighted pandas dataframes.
  :param spark: sparkSession object

  :type query: str
  :type authentication_dict: dict
  :type db: str
  :type container_name: str
  :type output_format: str
  :type spark: pyspark.sql.session.SparkSession
  """
  if output_format not in ['df', 'dfs']:
    sys.exit("Output format not supported!")

  if isinstance(spark, type(None)):
    spark = SparkSession.builder.appName('Cosmos DB sparksession').getOrCreate()
  
  cfg = {
    "spark.cosmos.accountEndpoint" : authentication_dict['account_url'],
    "spark.cosmos.accountKey" : authentication_dict['account_key'],
    "spark.cosmos.database" : db,
    "spark.cosmos.container" : container_name,
    "spark.cosmos.read.customQuery": query
  }

  result = spark.read.format("cosmos.oltp") \
        .options(**cfg) \
        .option("spark.cosmos.read.inferSchema.enabled", "true") \
        .option("spark.cosmos.read.inferSchema.samplingSize", 10000) \
        .load()

  if output_format == 'df':
    result = result.toPandas()

  return result