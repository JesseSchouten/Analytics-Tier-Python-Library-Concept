# Databricks notebook source
# MAGIC %sh    
# MAGIC 
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

# https://docs.databricks.com/data/data-sources/sql-databases.html
# Python standard library
import sys
import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse

# Python open source libraries
import pandas as pd
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, DoubleType, LongType, StructType, StructField
from pyspark.sql.functions import udf, array, lit, col

# Python DutchChannels libraries
from dc_azure_toolkit import adx
from data_sql.main import main as data_sql_main

# COMMAND ----------

# MAGIC %run "../../../authentication_objects"

# COMMAND ----------

def get_request_url(channel, type):
  """
  Outputs a request to which a GET request can be send for a platform. Takes the channel and corresponding country as parameters.
  
  :param channel: dutchchannels channel, for example nfn, wl and vg

  :type channel: str
  """
  supported_types = ['movie', 'episode']
  if type not in supported_types:
    return None
    
  return f"https://dc-{channel}-ms-catalog.axprod.net/v1/{type}"

def process_movie_event(movie_event, channel):
  """
  Returns the desired values from the raw movie event dictionary.
  
  :param movie_event: a single raw movie event returned by the API of axinom.
  :param channel: channel from which the data consists, for example nfn, tt or wl
  
  :type movie_event: dict
  :type channel: str
  """
  
  result = []
  result.append(movie_event['id'])
  result.append(movie_event['asset_type'])
  result.append(movie_event['asset_subtype'])
  result.append(channel)
  result.append(movie_event['title'])
  try:
    #result.append(str(movie_event['genres'][0]))
    genres = list(movie_event['genres'][0].values())[0]
    result.append(str(genres))
  except Exception:
    result.append('')
  tags = str(list(set(movie_event['tags'])))
  result.append(tags)
  return result

def transform_channel(x):
  if x == 'vg':
    subscription_channel = 'tt'.upper()
  elif x == 'lov':
    subscription_channel = 'wl'.upper()
  else:
    subscription_channel = x.upper()
  return subscription_channel

def get_movies(channel):
  payload={}
  
  url = get_request_url(channel.lower(), 'movie')

  response = requests.request("GET", url, data=payload).json()
  
  params = {'page': 1}  

  movies = requests.get(url, params=params)
  body = movies.json()

  processed_body = [process_movie_event(event, channel) for event in body['items']]
  payload_items = processed_body

  while len(body['items']) > 0:
      movies = requests.get(url, params=params)
      body = movies.json()
      processed_body = [process_movie_event(event, channel) for event in body['items']]
      payload_items = payload_items + processed_body
      params['page'] += 1
  return payload_items

def get_series(channel):
  query = """
  playtime
| extend videoId = videoEvent_videoId
| where videoId contains '1-1'
| distinct videoId
  """
  authentication_dict = ADXAuthenticator('uelprdadx').get_authentication_dict()
  episode_list = adx.select(query, authentication_dict, kustoDatabase = 'uds', df_format ='df')['videoId'].unique().tolist()

  payload_items = []
  for episode in episode_list:
    try:
      payload={}
      url = get_request_url(channel.lower(), 'episode')
      response = requests.request("GET", url, data=payload).json()

      episodes = requests.get(url + "/" + episode)
      body = episodes.json()

      processed_body = [process_movie_event(body, channel)]

      payload_items += processed_body
    except Exception as e:
      continue

  return payload_items
  
channel_id_dict = {'nfn':'NFN','vg':'TT','lov':'WL'}

ctx = data_sql_main(environment='prd', dbutils=dbutils)
catalog_service = ctx['CatalogService']

payload_items = []
for channel in channel_id_dict.keys():
  payload_items += get_movies(channel)
  payload_items += get_series(channel)
  
insert_passed = catalog_service.run_insert(payload_items, batch_size=100)

if insert_passed == False:
  sys.exit("Error, catalog data is not inserted!")

# COMMAND ----------


