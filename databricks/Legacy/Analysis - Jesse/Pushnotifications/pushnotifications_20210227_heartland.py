# Databricks notebook source
# MAGIC %pip install azure-kusto-data azure-kusto-ingest
# MAGIC %pip install matplotlib

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
import json

# COMMAND ----------

#prd:
cluster = "https://uelprdadx.westeurope.kusto.windows.net" # id 
client_id = "c4645ab0-02ef-4eb4-b9e4-6c099112d5ab" #
client_secret = ")V<bnbHiYsjfXhgX0YtTk<_2)@?3o3" #
authority_id = "3a1898d5-544c-434f-ba75-5eae95714e13" #AAD dctracking-prd-ci-app Tenant ID

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,client_id,client_secret, authority_id)

client = KustoClient(kcsb)

# COMMAND ----------

"""
How many people did we win back???
"""

date = '2021-03-9'
query = """
set truncationmaxsize=1048576000;
subscriptions
| where Date >= startofday(datetime({})) and Date < startofday(datetime({}) + 1d) 
| project customer_id = ['Customer.Id'], subscription_id = ['Subscription.Id'], subscription_plan_id = ['SubscriptionPlan.Id'], subscription_created_date = ['Subscription.CreatedDate'],subscription_end_date = ['Subscription.SubscriptionEnd']
| join kind = rightouter 
            (
            subscriptions
             | where Date >= startofday(datetime({})) and Date < startofday(datetime({}) + 1d) 
            | project customer_id = ['Customer.Id'], subscription_created_date = ['Subscription.CreatedDate']
            | summarize subscription_created_date = max(subscription_created_date) by customer_id
            ) on subscription_created_date, customer_id
| distinct subscription_created_date, customer_id, subscription_plan_id, subscription_id, subscription_end_date
| join kind = leftouter 
            (
            database("shared").SubscriptionPlan
            | project subscription_plan_id = SubscriptionPlanId, platform=strcat_delim(" ", Channel, Country)
            )on subscription_plan_id
|project-away subscription_plan_id1
|project customer_id, subscription_id, subscription_plan_id, channel_country = platform,subscription_end_date,subscription_created_date
""".format(date,date,date,date)
db='playground'
response = client.execute(db, query)
df_engagement = dataframe_from_result_table(response.primary_results[0])

# COMMAND ----------

def load_list_from_json(file_name):
    with open(file_name) as file:
        json_load = json.load(file)

    if type(json_load) is list:
        list_ = json_load
    elif type(json_load) is str:
        list_ = ast.literal_eval(json_load)   
    else:
        print('Error, we dont recognize the type, terminate')
        return []
    return list_
  
def mark_group(x):
    if x in control_group:
        return 'control_group'
    elif x in group_1:
        return 'experimental_group_1'
    return 'None'


control_group_paths = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/ex_customers/heartland_gratis_control.json"]
group_1_paths = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/ex_customers/heartland_gratis_experimental.json"]

control_group_paths_pn = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/control_group.json"]
group_1_paths_pn = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/experimental_group_NFN_NL_heartland.json"]

control_group =[]
group_1 = []
control_group_pn = []
group_1_pn = []

for control_group_path,group_1_path,control_group_paths_pn, group_1_paths_pn in zip(control_group_paths, group_1_paths,control_group_paths_pn, group_1_paths_pn):
    control_group += load_list_from_json(control_group_path)
    group_1 +=load_list_from_json(group_1_path)
    control_group_pn += load_list_from_json(control_group_paths_pn)
    group_1_pn +=load_list_from_json(group_1_paths_pn)


df_engagement['group'] = df_engagement['customer_id'].apply(lambda x: mark_group(x))
df_engagement = df_engagement[df_engagement['group'] != 'None']
df_engagement[['subscription_id','channel_country','group']].groupby(['channel_country','group']).count()

def mark_customer(x):
  if x in control_group_pn or x in group_1_pn:
    return 1
  else: 
    return 0

df_engagement['pushnotification'] = df_engagement['customer_id'].apply(lambda x: mark_customer(x))

# COMMAND ----------

df_engagement = df_engagement[df_engagement['pushnotification']==1]
control_group_size = len(control_group)
exp_group_size = len(group_1)
print("size of control group: {}".format(control_group_size))
print("size of experimental group: {}".format(exp_group_size))
aftersubscription_df = df_engagement[df_engagement['subscription_created_date'] > pd.Timestamp('2021-2-19T00', tz='UTC')]
print(aftersubscription_df[['subscription_id','group']].groupby('group').count())

aftersubscription_df_count = aftersubscription_df[['subscription_id','group']].groupby('group').count()

perc = aftersubscription_df_count.iloc[0]/control_group_size
print("control group subscribed percentage:{}".format(perc[0]))

perc = aftersubscription_df_count.iloc[1]/exp_group_size
print("experimental group subscribed percentage: {}".format(perc[0]))

# COMMAND ----------

df_engagement = df_engagement[df_engagement['pushnotification']==1]
control_group_size = len(list(set(control_group) & set(control_group_pn)))
exp_group_size = len(list(set(group_1) & set(group_1_pn)))
print("size of control group: {}".format(control_group_size))
print("size of experimental group: {}".format(exp_group_size))
aftersubscription_df = df_engagement[df_engagement['subscription_created_date'] > pd.Timestamp('2021-2-19T00', tz='UTC')]
print(aftersubscription_df[['subscription_id','group']].groupby('group').count())

aftersubscription_df_count = aftersubscription_df[['subscription_id','group']].groupby('group').count()

perc = aftersubscription_df_count.iloc[0]/control_group_size
print("control group subscribed percentage:{}".format(perc[0]))

perc = aftersubscription_df_count.iloc[1]/exp_group_size
print("experimental group subscribed percentage: {}".format(perc[0]))

# COMMAND ----------

date = '2021-3-1'
query = """
set truncationmaxsize=1048576000;
subscriptions
| where Date >= startofday(datetime({})) and Date < startofday(datetime({}) + 1d) 
| project customer_id = ['Customer.Id'], subscription_id = ['Subscription.Id'], subscription_plan_id = ['SubscriptionPlan.Id'], subscription_created_date = ['Subscription.CreatedDate']
| join kind = rightouter 
            (
            subscriptions
             | where Date >= startofday(datetime({})) and Date < startofday(datetime({}) + 1d) 
            | project customer_id = ['Customer.Id'], subscription_created_date = ['Subscription.CreatedDate']
            | summarize subscription_created_date = max(subscription_created_date) by customer_id
            ) on subscription_created_date, customer_id
| distinct subscription_created_date, customer_id, subscription_plan_id, subscription_id
| join kind = leftouter 
            (
            database("shared").SubscriptionPlan
            | project subscription_plan_id = SubscriptionPlanId, platform=strcat_delim(" ", Channel, Country)
            )on subscription_plan_id
|project-away subscription_plan_id1, subscription_created_date
|project customer_id, subscription_id, subscription_plan_id, channel_country = platform
""".format(date,date,date,date)
db='playground'
response = client.execute(db, query)
df_engagement = dataframe_from_result_table(response.primary_results[0])

# COMMAND ----------

def load_list_from_json(file_name):
    with open(file_name) as file:
        json_load = json.load(file)

    if type(json_load) is list:
        list_ = json_load
    elif type(json_load) is str:
        list_ = ast.literal_eval(json_load)   
    else:
        print('Error, we dont recognize the type, terminate')
        return []
    return list_
  
def mark_group(x):
    if x in control_group:
        return 'control_group'
    elif x in group_1:
        return 'experimental_group_1'
    return 'None'

control_group_paths = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/current_customers/heartland_betaald_control.json"]
group_1_paths = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/current_customers/heartland_betaald_experimental.json"]
  
control_group_paths_pn = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/control_group.json"]
group_1_paths_pn = ["/dbfs/FileStore/tables/20210227/NFN/NL_heartland/experimental_group_NFN_NL_heartland.json"]

control_group =[]
group_1 = []
control_group_pn = []
group_1_pn = []

for control_group_path,group_1_path,control_group_paths_pn, group_1_paths_pn in zip(control_group_paths, group_1_paths,control_group_paths_pn, group_1_paths_pn):
    control_group += load_list_from_json(control_group_path)
    group_1 +=load_list_from_json(group_1_path)
    control_group_pn += load_list_from_json(control_group_paths_pn)
    group_1_pn +=load_list_from_json(group_1_paths_pn)

df_engagement['group'] = df_engagement['customer_id'].apply(lambda x: mark_group(x))
df_engagement = df_engagement[df_engagement['group'] != 'None']
df_engagement[['subscription_id','channel_country','group']].groupby(['channel_country','group']).count()

def mark_customer(x):
  if x in control_group_pn or x in group_1_pn:
    return 1
  else: 
    return 0

df_engagement['pushnotification'] = df_engagement['customer_id'].apply(lambda x: mark_customer(x))

# COMMAND ----------

"""
Timezone in UTC, change to +1!!
"""
timezone_dict = {'NFN GB': 0, 'NFN IE':0, 'NFN SE':1, 'NFN NO':1, 'NFN NL':1, 'NFN AU':10,'NFN NZ':10,'WL NL': 1, 'WL BE':1, 'WL NO': 1, 'WL SE': 1}
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

"date_list = []
d = pd.Timestamp('2021-2-1T00', tz='UTC')
for i in range(1,40):
  date_list.append(d)
  d = d + timedelta(days = 1)
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


def mark_customer(x):
  if x in control_group_pn or x in group_1_pn:
    return 1
  else: return 0

df['pushnotification'] = df['userId'].apply(lambda x: mark_customer(x))
df

pn_only = 0

if pn_only:
  df = df[df['pushnotification'] == 0]

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

date_list = []
d = pd.Timestamp('2021-2-1T00', tz='UTC')
for i in range(1,40):
  date_list.append(d)
  d = d + timedelta(days = 1)

df_watchtime_selected = df[df['timestamp'].isin(date_list)]

df_merge = pd.merge(df_engagement_selected,df_watchtime_selected,how = 'right',left_on=['customer_id'],right_on=['userId'])
df_merge = df_merge.rename(columns = {'all':'nr_videostarts'})

#country_list = ['NFN GB','NFN IE','NFN NO','NFN SE', 'NFN NL']
#country_list = ['NFN GB', 'NFN IE']
#country_list = ['NFN AU']
#country_list = ['NFN NO']
country_list = ['NFN NL']
#country_list = ['WL NL', 'WL BE']
#country_list = ['WL NO']
#country_list = ['WL SE']
#country_list = ['WL NL', 'WL BE','WL NO', 'WL SE']

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

sns.distplot(control_group_df['nr_videostarts'],label='control_group', kde=False,color='red',bins=60)
sns.distplot(exp1_group_df['nr_videostarts'],label='experimental_group_1', kde=False,color='black',bins=60)
sns.distplot(exp2_group_df['nr_videostarts'],label='experimental_group_2', kde=False,color='green',bins=70)

display(df_merge[['group','nr_videostarts']])

plt.title('Videostarts . per group - NFN NL')
plt.legend()
plt.ylabel('Nr of users')

# COMMAND ----------

channel_list = df_engagement['channel_country'].unique().tolist()
df_pm = pd.DataFrame()

for channel in channel_list:
  print(f'Start channel {channel}')
  userid_list = df_engagement[df_engagement['channel_country'] == channel]['customer_id'].unique().tolist()
  userid_tuple = tuple(userid_list)
  print(len(userid_tuple))
  
  db = "uel"
  movie = 'heartland' 
  query_promoted_movie = """
  tracking
  | where videoId contains '{}' and userId in {} 
  | summarize playtime=round(toreal(sum(videoPlayTime)) / 60) by bin(timestamp + {}h, 1d), userId ;
  """.format(movie,userid_tuple,timezone_dict[channel])


  response = client.execute(db, query_promoted_movie)
  df_temp = dataframe_from_result_table(response.primary_results[0])
  df_pm = pd.concat((df_pm,df_temp))

movie_dict = {'1-0-we-three-kings':['NFN GB','NFN IE','NFN AU','NFN NZ','NFN NO','NFN SE']
             ,'1-1-sonja-en-jan-online-s01-e01':['NFN NL']
             ,'1-0-love-and-glamping':['WL NL']
             ,'1-0-very-very-valentine' :['WL NL', 'WL BE', 'WL NO', 'WL SE']
             ,'1-0-the-reason':['NFN GB','NFN IE','NFN NL','NFN NO','NFN SE']
              ,'1-1-heartland-s09-e01':['NFN NL']
              ,'heartland':['NFN NL']
              ,'heartland-s09':['NFN NL']
             }

df_pm= df_pm.rename(columns={"Column1":'timestamp'})

print(df_pm)


# COMMAND ----------

date_list = []
d = pd.Timestamp('2021-3-1T00', tz='UTC')
for i in range(1,10):
  date_list.append(d)
  d = d + timedelta(days = 1)

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

displayHTML("""<font size="6" color="black" face="sans-serif">Playtime for {}</font>""".format(movie))

# COMMAND ----------


#display(df_pm_merge[['group','timestamp','playtime']].groupby(['timestamp','group']).mean().reset_index().rename(columns={'playtime':'playtime_in_minutes'}))
def clean_groupname(x):
  if x == 'experimental_group_2':
    return 'automation'
  elif x == 'experimental_group_1':
    return 'automation'
  else:
    return x
  
def correction_factor(x):
  if x == 'automation':
    return 1
  else:
    return 799/3086

temp_df =df_pm_merge[['group','timestamp','playtime']]
temp_df['group'] = temp_df['group'].apply(lambda x: clean_groupname(x))
temp_df['correction_factor'] = temp_df['group'].apply(lambda x: correction_factor(x))
temp_df['playtime_corrected'] = temp_df['playtime'] / temp_df['correction_factor']
temp_df = temp_df[['group','timestamp','playtime_corrected']]
print(temp_df)
display(temp_df.groupby(['timestamp','group']).sum().reset_index().rename(columns={'playtime_corrected':'playtime_in_minutes'}))
del temp_df

# COMMAND ----------

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


