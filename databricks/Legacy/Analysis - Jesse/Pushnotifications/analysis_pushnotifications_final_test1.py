# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib

# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/nfn_nl_app_user_data_complete.csv")


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

query = "SELECT * FROM nfn_nl_app_user_data_complete_csv;"
dfs_engagement = spark.sql(query)
df_engagement = dfs_engagement.toPandas()
#df_engagement = df_engagement[df_engagement['exportDate'] == '2020-12-10']
userid_list = df_engagement['customer_id'].unique().tolist()
df_engagement[['group','customer_id']].groupby('group').nunique()
print("Nr records in df_engagement: {}".format(len(df_engagement)))
print(df_engagement[['group','customer_id']].groupby('group').nunique())

# COMMAND ----------

df_base = df_engagement[df_engagement['exportDate'] == '2020-12-10'][['customer_id', 'subscription_id', 'subscription_plan_id',
       'channel_country', 'group']]
df_base


# COMMAND ----------

df_engagement_aggr = df_engagement[['exportDate','group','engagementstatus','customer_id']].groupby(['exportDate','group','engagementstatus']).count().reset_index()
df_engagement_aggr[df_engagement_aggr['exportDate'] == '2020-12-18']

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

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC'),pd.Timestamp('2020-12-10T00', tz='UTC')
            ,pd.Timestamp('2020-12-11T00', tz='UTC'),pd.Timestamp('2020-12-12T00', tz='UTC'),pd.Timestamp('2020-12-13T00', tz='UTC')
            ,pd.Timestamp('2020-12-14T00', tz='UTC'),pd.Timestamp('2020-12-15T00', tz='UTC'),pd.Timestamp('2020-12-16T00', tz='UTC')
            ,pd.Timestamp('2020-12-17T00', tz='UTC'),pd.Timestamp('2020-12-18T00', tz='UTC'),pd.Timestamp('2020-12-19T00', tz='UTC')
            ,pd.Timestamp('2020-12-20T00', tz='UTC'),pd.Timestamp('2020-12-21T00', tz='UTC')]
#date_list = [datetime(2020,12,8,0,0,0),datetime(2020,12,9,0,0,0)]
df = df[df['timestamp'].isin(date_list)]
print(df)
type(date_list[1])


# COMMAND ----------

mux = pd.MultiIndex.from_product([pd.DatetimeIndex(date_list), 
                                  userid_list], names=['timestamp','userId'])

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index(['timestamp','userId']).reindex(mux, fill_value=0)
df= df.rename(columns={"video_id_count":'all'}).reset_index()
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
date_string = '2020-12-17'
df_engagement_selected = df_engagement[df_engagement['exportDate'] == date_string]
df_engagement_selected = df_engagement_selected[['customer_id','group','engagementstatus']]
df_engagement_selected = df_engagement_selected.rename(columns={'engagementstatus':'engagementstatus_{}'.format(date_string)})

date_list = [pd.Timestamp('2020-12-19T00', tz='UTC'),pd.Timestamp('2020-12-20T00', tz='UTC'),pd.Timestamp('2020-12-21T00', tz='UTC')]

#date_list = [pd.Timestamp('2020-12-16T00', tz='UTC'),pd.Timestamp('2020-12-17T00', tz='UTC'),pd.Timestamp('2020-12-18T00', tz='UTC')]

df_watchtime_selected = df[df['timestamp'].isin(date_list)]

df_merge = pd.merge(df_engagement_selected,df_watchtime_selected,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_merge = df_merge.rename(columns = {'all':'nr_videostarts'})

print("Evaluate engagementstatus on {} for watchtime data on {}".format(date_string,str(date_list)))

# COMMAND ----------

print(df_merge[['nr_videostarts','engagementstatus_{}'.format(date_string)]].groupby('engagementstatus_{}'.format(date_string)).mean())
print(df_merge[['customer_id','group']].groupby('group').count())
print(df_merge[['group','nr_videostarts']].groupby(['group']).sum())


# COMMAND ----------

import matplotlib.pyplot as plt

control_group_df = df_merge[df_merge['group'] == 'control_group']
exp1_group_df = df_merge[df_merge['group'] == 'experimental_group_1']
exp2_group_df = df_merge[df_merge['group'] == 'experimental_group_2']
print(control_group_df.describe())
print(control_group_df.median())
print(exp1_group_df.describe())
print(control_group_df.median())
print(exp2_group_df.describe())
print(control_group_df.median())
import scipy 
from scipy import stats
x = np.array(control_group_df['nr_videostarts'].tolist())
y = np.array(exp1_group_df['nr_videostarts'].tolist())
z = np.array(exp2_group_df['nr_videostarts'].tolist())
print("Kruskal wallis test for difference amongst the 3 groups.")
print(stats.kruskal(x,y,z))
print(stats.kruskal(x,y))
print(stats.kruskal(x,z))
print(stats.kruskal(y,z))
sns.distplot(control_group_df['nr_videostarts'],label='control_group', kde=False,color='blue',bins=60)
sns.distplot(exp1_group_df['nr_videostarts'],label='experimental_group_1', kde=False,color='red',bins=50)
sns.distplot(exp2_group_df['nr_videostarts'],label='experimental_group_2', kde=False,color='black',bins=50)
plt.title('Videostarts between 19 - 21 dec. per group')
plt.legend()
plt.ylabel('Nr of users')

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

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC'),pd.Timestamp('2020-12-10T00', tz='UTC')
            ,pd.Timestamp('2020-12-11T00', tz='UTC'),pd.Timestamp('2020-12-12T00', tz='UTC'),pd.Timestamp('2020-12-13T00', tz='UTC')
            ,pd.Timestamp('2020-12-14T00', tz='UTC'),pd.Timestamp('2020-12-15T00', tz='UTC'),pd.Timestamp('2020-12-16T00', tz='UTC')
             ,pd.Timestamp('2020-12-17T00', tz='UTC'),pd.Timestamp('2020-12-18T00', tz='UTC')
            ,pd.Timestamp('2020-12-19T00', tz='UTC'),pd.Timestamp('2020-12-20T00', tz='UTC')
            ]

df_wt = df_wt[df_wt['timestamp'].isin(date_list)]

# COMMAND ----------

date_string = '2020-12-17'
df_engagement_selected = df_engagement[df_engagement['exportDate'] == date_string]
df_engagement_selected = df_engagement_selected[['customer_id','group','engagementstatus']]
df_engagement_selected = df_engagement_selected.rename(columns={'engagementstatus':'engagementstatus_{}'.format(date_string)})
df_wt_merge = pd.merge(df_engagement_selected,df_wt,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_wt_merge = df_wt_merge.rename(columns = {'all':'mean_nr_videostarts'})

# COMMAND ----------

display(df_wt_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).mean().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))
display(df_wt_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).sum().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))


# COMMAND ----------

date_list = [pd.Timestamp('2020-12-16T00', tz='UTC'),pd.Timestamp('2020-12-17T00', tz='UTC'),pd.Timestamp('2020-12-18T00', tz='UTC')]

df_wt_merge_selected = df_wt_merge[df_wt_merge['timestamp'].isin(date_list)]
#df_wt_merge_selected = df_wt_merge_selected[df_wt_merge_selected['playtime'] > 0]

# COMMAND ----------

control_group_df_wt = df_wt_merge_selected[df_wt_merge_selected['group'] == 'control_group']
exp1_group_df_wt = df_wt_merge_selected[df_wt_merge_selected['group'] == 'experimental_group_1']
exp2_group_df_wt = df_wt_merge_selected[df_wt_merge_selected['group'] == 'experimental_group_2']
print(control_group_df_wt.describe())
print(exp1_group_df_wt.describe())
print(exp2_group_df_wt.describe())
print("Playtime on 2020-12-8 and 2020-12-9 per group after videoclick")
display(df_wt_merge_selected)

x = np.array(control_group_df_wt['playtime'])
y = np.array(exp1_group_df_wt['playtime'])
z = np.array(exp2_group_df_wt['playtime'])
print("Kruskal wallis test for difference amongst the 3 groups.")
print(stats.kruskal(x,y,z))
print(stats.kruskal(x,y))
print(stats.kruskal(x,z))
print(stats.kruskal(y,z))


# COMMAND ----------

query_promoted_movie = """
tracking
| where videoId == '1-0-christmas-grace' 
| summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by bin(timestamp + 1h, 1d), userId ;
"""
db= 'uel'
response = client.execute(db, query_promoted_movie)
df_pm = dataframe_from_result_table(response.primary_results[0])
df_pm = df_pm[df_pm['userId'].isin(userid_list)]
df_pm = df_pm.rename(columns={'Column1':'timestamp'})
print(df_pm)

# COMMAND ----------

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC'),pd.Timestamp('2020-12-10T00', tz='UTC')
            ,pd.Timestamp('2020-12-11T00', tz='UTC'),pd.Timestamp('2020-12-12T00', tz='UTC'),pd.Timestamp('2020-12-13T00', tz='UTC')
            ,pd.Timestamp('2020-12-14T00', tz='UTC'),pd.Timestamp('2020-12-15T00', tz='UTC'),pd.Timestamp('2020-12-16T00', tz='UTC')
             ,pd.Timestamp('2020-12-17T00', tz='UTC')
            ,pd.Timestamp('2020-12-18T00', tz='UTC'),pd.Timestamp('2020-12-19T00', tz='UTC'),pd.Timestamp('2020-12-20T00', tz='UTC')
            ]

df_pm = df_pm[df_pm['timestamp'].isin(date_list)]

# COMMAND ----------

date_string = '2020-12-16'
df_engagement_selected = df_engagement[df_engagement['exportDate'] == date_string]
df_engagement_selected = df_engagement_selected[['customer_id','group','engagementstatus']]
df_engagement_selected = df_engagement_selected.rename(columns={'engagementstatus':'engagementstatus_{}'.format(date_string)})
df_pm_merge = pd.merge(df_engagement_selected,df_pm,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_pm_merge =df_pm_merge.rename(columns = {'all':'mean_nr_videostarts'})
print(df_pm_merge[df_pm_merge['timestamp'] == '2020-12-16'][['customer_id','engagementstatus_2020-12-16','playtime']])
print(df_pm_merge[['customer_id','group','timestamp']].groupby(['timestamp','group']).count())

# COMMAND ----------

display(df_pm_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).mean().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))
display(df_pm_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).sum().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))


# COMMAND ----------

date_list = [pd.Timestamp('2020-12-16T00', tz='UTC'),pd.Timestamp('2020-12-17T00', tz='UTC')]

df_pm_merge_selected = df_pm_merge[df_pm_merge['timestamp'].isin(date_list)]
#df_pm_merge_selected = df_pm_merge_selected[df_pm_merge_selected['playtime'] > 0]


# COMMAND ----------

control_group_df_pm = df_pm_merge_selected[df_pm_merge_selected['group'] == 'control_group']
exp1_group_df_pm = df_pm_merge_selected[df_pm_merge_selected['group'] == 'experimental_group_1']
exp2_group_df_pm = df_pm_merge_selected[df_pm_merge_selected['group'] == 'experimental_group_2']
print(control_group_df_pm.describe())
print(exp1_group_df_pm.describe())
print(exp2_group_df_pm.describe())
print("Playtime on 2020-12-8 and 2020-12-9 per group after videoclick")
display(df_pm_merge_selected)

x = np.array(control_group_df_pm['playtime'])
y = np.array(exp1_group_df_pm['playtime'])
z = np.array(exp2_group_df_pm['playtime'])
print("Kruskal wallis test for difference amongst the 3 groups.")
print(stats.kruskal(x,y,z))
print(stats.kruskal(x,z))
print(stats.kruskal(x,y))
print(stats.kruskal(y,z))

# COMMAND ----------

display(exp2_group_df_pm)

# COMMAND ----------


