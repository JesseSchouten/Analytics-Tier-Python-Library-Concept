# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib

# COMMAND ----------

#dbutils.fs.rm("FileStore/tables/pushnotifications_test2_userbase.csv")



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
from scipy import stats

# COMMAND ----------

#prd:
cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" #
client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" #
authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,client_id,client_secret, authority_id)

client = KustoClient(kcsb)

# COMMAND ----------

query = "SELECT * FROM userbase_pushnotifications_20201226;"
dfs_engagement = spark.sql(query)
df_engagement = dfs_engagement.toPandas()
userid_list = df_engagement['customer_id'].unique().tolist()
userid_tuple = tuple(userid_list)
df_engagement[['group','customer_id']].groupby('group').nunique()
print("Nr records in df_engagement: {}".format(len(df_engagement)))
print(df_engagement[['group','customer_id']].groupby('group').nunique())
df_engagement

# COMMAND ----------

"""
Timezone in UTC, change to +1!!
"""
timezone_dict = {'NFN GB': 0, 'NFN IE':0, 'NFN SE':1, 'NFN NO':1, 'NFN NL':1, 'NFN AU':10,'NFN NZ':10}
channel_list = df_engagement['channel_country'].unique().tolist()
df = pd.DataFrame()

for channel in channel_list:
  print(f'Start channel {channel}')
  userid_list = df_engagement[df_engagement['channel_country'] == channel]['customer_id'].unique().tolist()
  userid_tuple = tuple(userid_list)
  
  db = "uel"
  query_videostarts = """
  tracking 
  | where userId in {} 
  | summarize video_id_count = dcount(videoId) by bin(timestamp + {}h,1d), userId;
  """.format(userid_tuple,timezone_dict[channel])


  response = client.execute(db, query_videostarts)
  df_temp = dataframe_from_result_table(response.primary_results[0])
  #df = df[df['userId'].isin(userid_list)]
  df = pd.concat((df,df_temp))

df= df.rename(columns={"Column1":'timestamp'})

print(df)


# COMMAND ----------

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC'),pd.Timestamp('2020-12-10T00', tz='UTC')
            ,pd.Timestamp('2020-12-11T00', tz='UTC'),pd.Timestamp('2020-12-12T00', tz='UTC'),pd.Timestamp('2020-12-13T00', tz='UTC')
            ,pd.Timestamp('2020-12-14T00', tz='UTC'),pd.Timestamp('2020-12-15T00', tz='UTC'),pd.Timestamp('2020-12-16T00', tz='UTC')
            ,pd.Timestamp('2020-12-17T00', tz='UTC'),pd.Timestamp('2020-12-18T00', tz='UTC'),pd.Timestamp('2020-12-19T00', tz='UTC')
            ,pd.Timestamp('2020-12-20T00', tz='UTC'),pd.Timestamp('2020-12-21T00', tz='UTC'), pd.Timestamp('2020-12-22T00', tz='UTC')
            ,pd.Timestamp('2020-12-23T00', tz='UTC'),pd.Timestamp('2020-12-24T00', tz='UTC'),pd.Timestamp('2020-12-25T00', tz='UTC'),
            pd.Timestamp('2020-12-26T00', tz='UTC'),pd.Timestamp('2020-12-27T00', tz='UTC'),pd.Timestamp('2020-12-28T00', tz='UTC')
            ,pd.Timestamp('2020-12-29T00', tz='UTC'),pd.Timestamp('2020-12-30T00', tz='UTC'),pd.Timestamp('2020-12-31T00', tz='UTC')]
#date_list = [datetime(2020,12,8,0,0,0),datetime(2020,12,9,0,0,0)]
df = df[df['timestamp'].isin(date_list)]
print(df)
type(date_list[1])


# COMMAND ----------

userid_list = df_engagement['customer_id'].unique().tolist()

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

df_engagement_selected = df_engagement[['customer_id','group','channel_country']]

date_list = [pd.Timestamp('2020-12-27T00', tz='UTC'), pd.Timestamp('2020-12-28T00', tz='UTC'), pd.Timestamp('2020-12-29T00', tz='UTC')]

df_watchtime_selected = df[df['timestamp'].isin(date_list)]

df_merge = pd.merge(df_engagement_selected,df_watchtime_selected,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_merge = df_merge.rename(columns = {'all':'nr_videostarts'})

country_list = ['NFN GB','NFN IE','NFN AU','NFN NZ','NFN NO','NFN SE']
#country_list = ['NFN GB', 'NFN IE']
#country_list = ['NFN AU']
#country_list = ['NFN SE']
#country_list = ['NFN NL']
df_merge=df_merge[df_merge['channel_country'].isin(country_list)]
print("watchtime data on {}".format(str(date_list)))

# COMMAND ----------

print(df_merge[['customer_id','group']].groupby('group').count())
print(df_merge[['group','nr_videostarts']].groupby(['group']).sum())

print(df_merge[df_merge['group'] == 'control_group']['nr_videostarts'].value_counts(sort=True))
print(df_merge[df_merge['group'] == 'experimental_group_1']['nr_videostarts'].value_counts(sort=True))
print(df_merge[df_merge['group'] == 'experimental_group_2']['nr_videostarts'].value_counts(sort=True))

# COMMAND ----------

control_group_df = df_merge[df_merge['group'] == 'control_group']
exp1_group_df = df_merge[df_merge['group'] == 'experimental_group_1']
exp2_group_df = df_merge[df_merge['group'] == 'experimental_group_2']
print(control_group_df.describe())
print(control_group_df.median())
print(exp1_group_df.describe())
print(control_group_df.median())
print(exp2_group_df.describe())
print(control_group_df.median())

x = np.array(control_group_df['nr_videostarts'].tolist())
y = np.array(exp1_group_df['nr_videostarts'].tolist())
z = np.array(exp2_group_df['nr_videostarts'].tolist())
print("Kruskal wallis test for difference amongst the 3 groups.")
print(stats.kruskal(x,y,z))
print(stats.kruskal(x,y))
print(stats.kruskal(x,z))
print(stats.kruskal(y,z))


sns.distplot(exp1_group_df['nr_videostarts'],label='experimental_group_1', kde=False,color='black',bins=60)
sns.distplot(exp2_group_df['nr_videostarts'],label='experimental_group_2', kde=False,color='green',bins=70)
sns.distplot(control_group_df['nr_videostarts'],label='control_group', kde=False,color='red',bins=60)

display(df_merge[['group','nr_videostarts']])

plt.title('Videostarts between 27 - 29 dec. per group - NFN non NL')
plt.legend()
plt.ylabel('Nr of users')

# COMMAND ----------

"""
Timezone in UTC, change to +1!!
"""
timezone_dict = {'NFN GB': 0, 'NFN IE':0, 'NFN SE':1, 'NFN NO':1, 'NFN NL':1, 'NFN AU':10,'NFN NZ':10}
channel_list = df_engagement['channel_country'].unique().tolist()
df_wt = pd.DataFrame()

for channel in channel_list:
  print(f'Start channel {channel}')
  userid_list = df_engagement[df_engagement['channel_country'] == channel]['customer_id'].unique().tolist()
  userid_tuple = tuple(userid_list)
  
  db = "uel"
  query_watchtime = """
  tracking
  | where userId in {} 
  | summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by bin(timestamp + {}h, 1d), userId;
  """.format(userid_tuple,timezone_dict[channel])


  response = client.execute(db, query_watchtime)
  df_temp = dataframe_from_result_table(response.primary_results[0])
  #df = df[df['userId'].isin(userid_list)]
  df_wt = pd.concat((df_wt,df_temp))

df_wt= df_wt.rename(columns={"Column1":'timestamp'})

print(df_wt)

# COMMAND ----------

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC'),pd.Timestamp('2020-12-10T00', tz='UTC')
            ,pd.Timestamp('2020-12-11T00', tz='UTC'),pd.Timestamp('2020-12-12T00', tz='UTC'),pd.Timestamp('2020-12-13T00', tz='UTC')
            ,pd.Timestamp('2020-12-14T00', tz='UTC'),pd.Timestamp('2020-12-15T00', tz='UTC'),pd.Timestamp('2020-12-16T00', tz='UTC')
            ,pd.Timestamp('2020-12-17T00', tz='UTC'),pd.Timestamp('2020-12-18T00', tz='UTC'),pd.Timestamp('2020-12-19T00', tz='UTC')
            ,pd.Timestamp('2020-12-20T00', tz='UTC'),pd.Timestamp('2020-12-21T00', tz='UTC'), pd.Timestamp('2020-12-22T00', tz='UTC')
            ,pd.Timestamp('2020-12-23T00', tz='UTC'),pd.Timestamp('2020-12-24T00', tz='UTC'),pd.Timestamp('2020-12-25T00', tz='UTC'),
            pd.Timestamp('2020-12-26T00', tz='UTC'),pd.Timestamp('2020-12-27T00', tz='UTC'),pd.Timestamp('2020-12-28T00', tz='UTC')
            ,pd.Timestamp('2020-12-29T00', tz='UTC'),pd.Timestamp('2020-12-30T00', tz='UTC'),pd.Timestamp('2020-12-31T00', tz='UTC')]

df_wt = df_wt[df_wt['timestamp'].isin(date_list)]

# COMMAND ----------

df_engagement_selected = df_engagement[['customer_id','group']]

df_wt_merge = pd.merge(df_engagement_selected,df_wt,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_wt_merge = df_wt_merge.rename(columns = {'all':'mean_nr_videostarts'})

# COMMAND ----------

display(df_wt_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).mean().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))
display(df_wt_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).sum().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))


# COMMAND ----------

date_list = [pd.Timestamp('2020-12-27T00', tz='UTC'),pd.Timestamp('2020-12-28T00', tz='UTC'),pd.Timestamp('2020-12-29T00', tz='UTC')]

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

timezone_dict = {'NFN GB': 0, 'NFN IE':0, 'NFN SE':1, 'NFN NO':1, 'NFN NL':1, 'NFN AU':10,'NFN NZ':10}
channel_list = df_engagement['channel_country'].unique().tolist()
df_pm = pd.DataFrame()

for channel in channel_list:
  print(f'Start channel {channel}')
  userid_list = df_engagement[df_engagement['channel_country'] == channel]['customer_id'].unique().tolist()
  userid_tuple = tuple(userid_list)
  print(len(userid_tuple))
  
  db = "uel"
  movie = '1-0-we-three-kings' 
  #movie = '1-1-sonja-en-jan-online-s01-e01'
  query_promoted_movie = """
  tracking
  | where videoId == '{}' and userId in {} 
  | summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by bin(timestamp + {}h, 1d), userId ;
  """.format(movie,userid_tuple,timezone_dict[channel])


  response = client.execute(db, query_promoted_movie)
  df_temp = dataframe_from_result_table(response.primary_results[0])
  #df = df[df['userId'].isin(userid_list)]
  df_pm = pd.concat((df_pm,df_temp))

movie_dict = {'1-0-we-three-kings':['NFN GB','NFN IE','NFN AU','NFN NZ','NFN NO','NFN SE']
             ,'1-1-sonja-en-jan-online-s01-e01':['NFN NL']}

df_pm= df_pm.rename(columns={"Column1":'timestamp'})

print(df_pm)


# COMMAND ----------

date_list = [pd.Timestamp('2020-12-8T00', tz='UTC'),pd.Timestamp('2020-12-9T00', tz='UTC'),pd.Timestamp('2020-12-10T00', tz='UTC')
            ,pd.Timestamp('2020-12-11T00', tz='UTC'),pd.Timestamp('2020-12-12T00', tz='UTC'),pd.Timestamp('2020-12-13T00', tz='UTC')
            ,pd.Timestamp('2020-12-14T00', tz='UTC'),pd.Timestamp('2020-12-15T00', tz='UTC'),pd.Timestamp('2020-12-16T00', tz='UTC')
            ,pd.Timestamp('2020-12-17T00', tz='UTC'),pd.Timestamp('2020-12-18T00', tz='UTC'),pd.Timestamp('2020-12-19T00', tz='UTC')
            ,pd.Timestamp('2020-12-20T00', tz='UTC'),pd.Timestamp('2020-12-21T00', tz='UTC'), pd.Timestamp('2020-12-22T00', tz='UTC')
            ,pd.Timestamp('2020-12-23T00', tz='UTC'),pd.Timestamp('2020-12-24T00', tz='UTC'),pd.Timestamp('2020-12-25T00', tz='UTC'),
            pd.Timestamp('2020-12-26T00', tz='UTC'),pd.Timestamp('2020-12-27T00', tz='UTC'),pd.Timestamp('2020-12-28T00', tz='UTC')
            ,pd.Timestamp('2020-12-29T00', tz='UTC'),pd.Timestamp('2020-12-30T00', tz='UTC'),pd.Timestamp('2020-12-31T00', tz='UTC')]


df_pm = df_pm[df_pm['timestamp'].isin(date_list)]

# COMMAND ----------

userid_list = df_engagement['customer_id'].unique().tolist()

df_engagement_selected = df_engagement[['customer_id','group','channel_country']]
df_engagement_selected = df_engagement_selected[df_engagement_selected['channel_country'].isin(movie_dict[movie])]
print(df_engagement_selected['channel_country'].unique().tolist())
df_pm_merge = pd.merge(df_engagement_selected,df_pm,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_pm_merge =df_pm_merge.rename(columns = {'all':'mean_nr_videostarts'})
print(df_pm_merge[['customer_id','group','timestamp']].groupby(['timestamp','group']).count())
print(df_pm_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).sum())

# COMMAND ----------

displayHTML("""<font size="6" color="black" face="sans-serif">Playtime for movie {}</font>""".format(movie))

# COMMAND ----------

#display(df_pm_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).mean().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))
def clean_groupname(x):
  if x == 'experimental_group_2':
    return 'movie PN'
  elif x == 'experimental_group_1':
    return 'general PN'
  else:
    return x
temp_df =df_pm_merge[['group','timestamp','playtime']]
temp_df['group'] = temp_df['group'].apply(lambda x: clean_groupname(x))
display(temp_df.groupby(['timestamp','group']).sum().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))
del temp_df

# COMMAND ----------

date_list = [pd.Timestamp('2020-12-27T00', tz='UTC'),pd.Timestamp('2020-12-28T00', tz='UTC')
            ,pd.Timestamp('2020-12-29T00', tz='UTC')]

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

display(df_pm_merge[df_pm_merge['group'] == 'experimental_group_2'][['group','timestamp','playtime']])

# COMMAND ----------


