# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib

# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/subscription_plan_id.csv")

# COMMAND ----------

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

import pandas as pd
import numpy as np
import itertools
from datetime import timedelta,datetime
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

#prd:
cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" #
client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" #
authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,client_id,client_secret, authority_id)

client = KustoClient(kcsb)

# COMMAND ----------

query = "SELECT * FROM nfn_nl_app_user_data_csv;"
dfs_engagement = spark.sql(query)
df_engagement = dfs_engagement.toPandas()
df_engagement = df_engagement[df_engagement['exportDate'] == '2020-12-10']
userid_list = df_engagement['customer_id'].tolist()

# COMMAND ----------

"""
Timezone in UTC, change to +1!!
"""
db = "uel"
query_videostarts = """
tracking 
| summarize video_id_count = dcount(videoId) by bin(timestamp + 1h,1d), userId;
"""

response = client.execute(db, query_videostarts)
df = dataframe_from_result_table(response.primary_results[0])
df = df[df['userId'].isin(userid_list)]
df= df.rename(columns={"Column1":'timestamp'})
print(df)

# COMMAND ----------

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC')]
#date_list = [datetime(2020,12,8,0,0,0),datetime(2020,12,9,0,0,0)]
df = df[df['timestamp'].isin(date_list)]
print(df)
type(date_list[1])


# COMMAND ----------

mux = pd.MultiIndex.from_product([pd.DatetimeIndex(date_list), 
                                  userid_list], names=['timestamp','userId'])

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index(['timestamp','userId']).reindex(mux, fill_value=0)
df= df.rename(columns={"video_id_count":'all'})
df

# COMMAND ----------

category = 'all'
fig,axes = plt.subplots(nrows=1, ncols=1, sharex=True, sharey=True)
#df.hist(column = 'all',bins=30,ax=axes)
sns.distplot(df['all'], kde=False,color='blue',bins=30)
plt.suptitle('#Daily videostarts', x=0.5, y=1.05, ha='center', fontsize='xx-large')
print("Mean number of clicks: {}".format(df['all'].mean()))
print("Median number of clicks: {}".format(df['all'].median()))
print("SD of number of clicks: {}".format(df['all'].std()))


# COMMAND ----------

df.reset_index()
date_string = '2020-12-10'
df_engagement_selected = df_engagement[df_engagement['exportDate'] == date_string]
df_engagement_selected = df_engagement_selected[['customer_id','group','engagementstatus']]
df_engagement_selected = df_engagement_selected.rename(columns={'engagementstatus':'engagementstatus_{}'.format(date_string)})
df_merge = pd.merge(df_engagement_selected,df,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_merge = df_merge.rename(columns = {'all':'mean_nr_videostarts'})

# COMMAND ----------

df_merge[['customer_id','group']].groupby('group').count()

# COMMAND ----------

print(df_merge[['mean_nr_videostarts','engagementstatus_2020-12-10']].groupby('engagementstatus_2020-12-10').mean())

# COMMAND ----------

import matplotlib.pyplot as plt

control_group_df = df_merge[df_merge['group'] == 'control_group']
exp1_group_df = df_merge[df_merge['group'] == 'experimental_group_1']
exp2_group_df = df_merge[df_merge['group'] == 'experimental_group_2']
print(control_group_df.describe())
print(exp1_group_df.describe())
print(exp2_group_df.describe())
import scipy 
from scipy import stats
x = np.array(control_group_df['mean_nr_videostarts'].tolist())
y = np.array(exp1_group_df['mean_nr_videostarts'].tolist())
z = np.array(exp2_group_df['mean_nr_videostarts'].tolist())
print("Kruskal wallis test for difference amongst the 3 groups.")
print(stats.kruskal(x,y,z))
sns.distplot(control_group_df['mean_nr_videostarts'],label='control_group', kde=False,color='blue',bins=40)
sns.distplot(exp1_group_df['mean_nr_videostarts'],label='experimental_group_1', kde=False,color='red',bins=40)
sns.distplot(exp2_group_df['mean_nr_videostarts'],label='experimental_group_2', kde=False,color='black',bins=50)
plt.title('Histogram per group')
plt.legend()

# COMMAND ----------

print(df_merge['engagementstatus_2020-12-10'].unique())
#es = 'engaged'
#df_merge_es = df_merge[df_merge['engagementstatus_2020-12-10'] == es]
df_merge_es = df_merge
control_group_df = df_merge_es[df_merge_es['group'] == 'control_group']
exp1_group_df = df_merge_es[df_merge_es['group'] == 'experimental_group_1']
exp2_group_df = df_merge_es[df_merge_es['group'] == 'experimental_group_2']
print(control_group_df.describe())
print(exp1_group_df.describe())
print(exp2_group_df.describe())

kde = True
hist = False
sns.distplot(control_group_df['mean_nr_videostarts'],label='control_group',hist=hist, kde=kde,kde_kws={'bw':1},color='blue',bins=40)
sns.distplot(exp1_group_df['mean_nr_videostarts'],label='experimental_group_1',hist=hist, kde=kde,color='red',kde_kws={'bw':1},bins=40)
sns.distplot(exp2_group_df['mean_nr_videostarts'],label='experimental_group_2',hist=hist, kde=kde,color='black',kde_kws={'bw':1},bins=50)
plt.title('Probability density function per group')
plt.legend()

# COMMAND ----------

query_watchtime = """
tracking
| summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by bin(timestamp + 1h, 1d), userId;
"""

response = client.execute(db, query_watchtime)
df_wt = dataframe_from_result_table(response.primary_results[0])
df_wt = df_wt[df_wt['userId'].isin(userid_list)]
df_wt = df_wt.rename(columns={'Column1':'timestamp'})
print(df_wt)

# COMMAND ----------

date_list = [pd.Timestamp('2020-12-1T00', tz='UTC'),pd.Timestamp('2020-12-2T00', tz='UTC'),pd.Timestamp('2020-12-3T00', tz='UTC'),pd.Timestamp('2020-12-4T00', tz='UTC'),pd.Timestamp('2020-12-5T00', tz='UTC')
  ,pd.Timestamp('2020-12-6T00', tz='UTC'),pd.Timestamp('2020-12-7T00', tz='UTC'),pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC'),pd.Timestamp('2020-12-10T00', tz='UTC')]

df_wt = df_wt[df_wt['timestamp'].isin(date_list)]

# COMMAND ----------

date_string = '2020-12-10'
#df_engagement_selected = df_engagement[df_engagement['exportDate'] == date_string]
#df_engagement_selected = df_engagement_selected[['customer_id','group','engagementstatus']]
#df_engagement_selected = df_engagement_selected.rename(columns={'engagementstatus':'engagementstatus_{}'.format(date_string)})
df_wt_merge = pd.merge(df_engagement_selected,df_wt,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_wt_merge = df_wt_merge.rename(columns = {'all':'mean_nr_videostarts'})

# COMMAND ----------

display(df_wt_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).mean().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))
display(df_wt_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).sum().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))


# COMMAND ----------

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC')]

df_wt_merge = df_wt_merge[df_wt_merge['timestamp'].isin(date_list)]
df_wt_merge = df_wt_merge[df_wt_merge['playtime'] > 0]

# COMMAND ----------

control_group_df_wt = df_wt_merge[df_wt_merge['group'] == 'control_group']
exp1_group_df_wt = df_wt_merge[df_wt_merge['group'] == 'experimental_group_1']
exp2_group_df_wt = df_wt_merge[df_wt_merge['group'] == 'experimental_group_2']
print(control_group_df_wt.describe())
print(exp1_group_df_wt.describe())
print(exp2_group_df_wt.describe())
print("Playtime on 2020-12-8 and 2020-12-9 per group after videoclick")
display(df_wt_merge)

# COMMAND ----------


