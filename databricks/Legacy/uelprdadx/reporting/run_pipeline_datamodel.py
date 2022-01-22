# Databricks notebook source
print("Load billing frequency table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/billing_frequency", 3600)
print("status: {}".format(status))

print("Load country table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/country", 3600)
print("status: {}".format(status))

print("Load subscription_plan table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/subscription_plan", 3600)
print("status: {}".format(status))

print("Load subscriptions table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/subscriptions_static", 3600)
print("status: {}".format(status))

print("Load subscriptions_daily table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/subscriptions_daily", 3600)
print("status: {}".format(status))

print("Load video table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/video", 3600)
print("status: {}".format(status))

print("Load video_genre table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/video_genre", 3600)
print("status: {}".format(status))

print("Load viewing table to reporting cluster of data explorer:")
status = dbutils.notebook.run("/Shared/ETL/uelprdadx/reporting/viewing", 3600)
print("status: {}".format(status))


# COMMAND ----------


