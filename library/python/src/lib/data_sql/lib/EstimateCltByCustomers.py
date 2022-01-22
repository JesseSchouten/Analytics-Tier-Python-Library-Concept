# Databricks notebook source
import pandas as pd
import numpy as np
from datetime import date
from scipy.optimize import minimize

from dc_azure_toolkit import sql

# COMMAND ----------

class EstimateCltByCustomers():
  def __init__(self, ctx):
    self.ctx = ctx
    self.authentication_dict = ctx['SQLAuthenticator'].get_authentication_dict()
    
  def get_subscriptions_of_customers(self, customer_list):
    """Retrieve the subscriptions (data) linked to the given customers. Also do some data preparation.

    :param customer_list: list with customer UUID's
    :type customer_list: list
    """
    query = """ SELECT \
    t1.customer_id AS customer_id, \
    t1.channel AS channel, \
    t1.country AS country, \
    t1.subscription_id AS subscription_id, \
    t1.subscription_create_date AS subscription_created_date, \
    t1.subscription_end AS subscription_end_date, \
    t1.subscription_plan_id AS subscription_plan_id, \
    t1.state AS state, \
    t1.recurring_enabled AS recurring_enabled, \
    t2.registration_country AS registration_country, \
    t1.first_payment_date AS first_payment_date \
    FROM subscriptions t1 \
    INNER JOIN customers t2 ON t1.customer_id = t2.customer_id 
    WHERE t1.customer_id in """ + str(tuple(customer_list))
    subscriptions_df = sql.select(query, self.authentication_dict, df_format='df')

    subscriptions_df = subscriptions_df[subscriptions_df['state'] != 'pending'] # remove pending
    subscriptions_df = subscriptions_df[subscriptions_df['customer_id'] != '00000000-0000-0000-0000-000000000000'] # remove fake customers
    subscriptions_df = subscriptions_df[subscriptions_df['subscription_id'] != '00000000-0000-0000-0000-000000000000'] # remove fake subs
    subscriptions_df = subscriptions_df[pd.notnull(subscriptions_df['first_payment_date'])] # remove rows with no payment
    subscriptions_df['first_payment_date'] = subscriptions_df['first_payment_date'].apply(lambda x: x.date()) # rounding to YYYY-MM-DD
    subscriptions_df['country'].loc[subscriptions_df['country'] == 'IE'] = 'GB' # changing IE into GB to merge into one country
    subscriptions_df['country'].loc[(subscriptions_df['channel'] == 'lov') & (subscriptions_df['country'] == 'NL') & (subscriptions_df['registration_country'] == 'BE')] = 'BE' # adding registration country BE from country NL to country BE - only for channel lov
    subscriptions_df['days_paid'] = (subscriptions_df['subscription_end_date']-subscriptions_df['first_payment_date']).dt.days # paid days
    return subscriptions_df

  def add_billing_frequency(self, subscriptions_df):
    """Retrieve the subscription_plan (data). Than add (merge) billing_frequency in the subscriptions data.

    :param subscriptions_df: subscriptions dataframe with column subscription_plan_id
    :type subscriptions_df: pd.DataFrame
    """

    query_plan_id = """ SELECT \
    id AS plan_id, \
    billing_frequency \
    FROM subscription_plan
    ;"""
    plan_id_df = sql.select(query_plan_id, self.authentication_dict, df_format = 'df')

    subscriptions_df = pd.merge(subscriptions_df, plan_id_df, left_on = 'subscription_plan_id', right_on = 'plan_id') # merge billing frequency into dataset
    subscriptions_df['billing_frequency'] = np.where(subscriptions_df['billing_frequency'] == 1, 30, subscriptions_df['billing_frequency']) # change billing frequency 1 to 30
    return subscriptions_df
  
  def get_data_on_customer_level(self, subscriptions_df, cycle_length):
    """Bring back the subcriptions dataframe to a customer level.

    :param subscriptions_df: subscriptions dataframe
    :param cycle_length: the cycle length in days to use to calculate billing_cycles (paid cycles of customer)

    :type subscriptions_df: pd.DataFrame
    :type cycle_length: int
    """

    customer_df = subscriptions_df.groupby('customer_id').agg({'first_payment_date': 'min', 'subscription_end_date': 'max', 'days_paid': 'sum', 'billing_frequency': 'max', 'subscription_id': 'count'}).reset_index()
    customer_df = customer_df.loc[customer_df['subscription_id']<2]
    customer_df['cohort'] = customer_df['first_payment_date'].apply(lambda x: x.strftime('%Y-%m'))
    customer_df = pd.merge(customer_df, subscriptions_df[['customer_id', 'subscription_end_date', 'recurring_enabled', 'channel', 'country']], on=['customer_id', 'subscription_end_date'])

    customer_df['billing_cycles'] = np.round(customer_df['days_paid']/cycle_length, decimals=0)
    customer_df['billing_cycles'] = customer_df['billing_cycles']-1
    return customer_df

  def e_clt(self, alpha, beta, pred_t):
    """
    Calculating the retention rate and survival curve with the given alpha and beta.

    - Rt = retention rate
    - St = Survival curve    

    :param alpha: the alpha to use
    :param beta: the beta to use
    :param pred_t: the length of the prediction

    :type alpha: float
    :type beta: float
    :type pred_t: int
    """
    t = list(range(0, pred_t + 1))
    Rt = []
    St = []

    for i in t:
        if i == 0: 
            Rt.append(0)
            St.append(1)
        else:
            Rt.append((beta + i - 1)/(alpha + beta + i - 1))
            St.append(Rt[i] * St[i - 1])

    clt = sum(St)

    return clt, St

  def minimiza_alpha_beta(self, active_cust, lost_cust, params=np.array((1,1))):
    """Function to retrieve the 'best' alpha and beta.

    :param active_subs: list with the active customers per billing cycle (cycle > 0)
    :param lost_subs: list with the lost customers per billing cycle (cycle > 0)
    :param customers_at_start: The number of customers at the start (t=0)
    :param params: start values to use for alpha and beta

    :type active_subs: list
    :type lost_subs: list
    :type customers_at_start: int
    :type params: np.array
    """
    res_temp = minimize(
      self.negative_loglikelihood,
      args = (active_cust, lost_cust), #parameters we do not optimize on
      method = 'Nelder-Mead',
      tol = 1e-13,
      x0 = params, #starting value of paramaters 
      bounds = [(0, None), (0, None)])

    alpha, beta = res_temp.x
    return alpha, beta

  def loglikelihood(self, params, active_list, lost_list):
    """Log likelihood function: Sum_t lost_t * ln(P_t) + active_lastCycle ln(S_lastCycle)

    :param params: start values to use for alpha and beta
    :param active_list: list with the active customers per billing cycle (cycle > 0)
    :param lost_list: list with the lost customers per billing cycle (cycle > 0)

    :type params: np.array
    :type active_list: list
    :type lost_list: list
    """
    t = list(range(1, len(active_list) + 1)) 
    alpha_temp, beta_temp = params
    Pt = []
    St = []
    ll = []

    for i in t:
      if i == 1:
        Pt.append(alpha_temp /(alpha_temp + beta_temp))
        St.append(1 - Pt[0])
      else:
        Pt.append(((beta_temp + i - 2)/(alpha_temp + beta_temp + i - 1)) * Pt[i - 2])
        St.append(St[i - 2] - Pt[i - 1])

      ll.append(lost_list[i-1] * np.log(Pt[i-1]))

    ll.append((active_list[-2]-lost_list[-1]) * np.log(St[-1]))

    return ll

  def negative_loglikelihood(self, params, active_list, lost_list):
    """Taking the sum of the negative log likelihood.

    :param params: start values to use for alpha and beta
    :param active_list: list with the active customers per billing cycle (cycle > 0)
    :param lost_list: list with the lost customers per billing cycle (cycle > 0)

    :type params: np.array
    :type active_list: list
    :type lost_list: list
    """
    return -(np.sum(self.loglikelihood(params, active_list, lost_list)))

  def simple_moving_average(self, data, length_prediction, window_actual=6):
    """Taking a moving average over the last window_actual churn values

    :param data: pd.series with churn values
    :param length_prediction: length of the predicted churn values
    :param window_actual: length of the window to take (in the moving average)

    :type data: pd.Series
    :type length_prediction: int
    :type window_actual: int
    """
    actual_churn = data.values
    length_pred = length_prediction-len(actual_churn)
    churn_list = actual_churn.tolist()

    for t in range(length_pred):
      length = len(churn_list)
      if length < window_actual:
        window = length
      else:
        window = window_actual
      yhat = np.nanmean([churn_list[i] for i in range(length-window,length)])
      churn_list.append(yhat)
    return churn_list

  def active_subs(self, data): 
    """Calculating active, lost customers, latest subscription end date, if all customers had change to churn, 
    actual churn and survival rate per billing cycle.

    :param data: pd.DataFrame with data on customer level

    :type data: pd.DataFrame
    """
    all_cycles = range(int(np.min(data['billing_cycles'])), int(np.max(data['billing_cycles']))+1)
    result = pd.DataFrame(data={'billing_cycles': list(all_cycles)})

    df_lost_customer = data.loc[data['recurring_enabled']==False]
    lost_per_month = df_lost_customer.groupby(by=df_lost_customer['billing_cycles']).size().reset_index(name = 'lost')
    result = pd.merge(result, lost_per_month, left_on = ['billing_cycles'], right_on = ['billing_cycles'], how = 'left')
    result['lost'].fillna(0, inplace=True)

    active_per_month = data.groupby(by=data['billing_cycles']).size().reset_index(name = 'active')
    result = pd.merge(result, active_per_month, left_on = ['billing_cycles'], right_on = ['billing_cycles'], how = 'left')
    result['active'] = np.nancumsum(result.loc[::-1, 'active'])[::-1]

    latest_end_per_month = data[['billing_cycles', 'subscription_end_date']].groupby(by=data['billing_cycles']).agg('max')['subscription_end_date'].reset_index(name = 'latest_end')
    today = date.today()
    latest_end_per_month['all_change_churning'] = latest_end_per_month['latest_end'] < today

    result['lost'] = result['lost'].shift(periods=1)
    result = pd.merge(result, latest_end_per_month, left_on = ['billing_cycles'], right_on = ['billing_cycles'], how = 'left')
    result['actual_churn'] = (result['lost']/result['active'].shift(periods=1))
    result['actual_survival'] = np.append([1.0], np.cumprod(1-result['actual_churn']).to_list()[1:])
    return result

  def get_estimate_clt_and_surv_rate_by_customers(self, series_of_customer_ids, length_prediction, cycle_length):
    """Estimate the clt and survival rate of the given customer list.
    The survival rate and clt are calculated in two ways: linear (moving average of the churn) and fader hardie (shifted-beta-geometric)

    :param series_of_customer_ids: list with customer UUID's
    :param length_prediction: length of the predicted churn values
    :param cycle_length: the length to take for 1 billing cycle

    :type series_of_customer_ids: list
    :type length_prediction: int
    :type cycle_length: int

    e.g.
    customer_id_list = ['F3F440C0-DDAE-4330-8369-000020E49A9D', '834E35BD-2217-436E-9EDF-000023DAB3CA',
                        '5D48DA63-366E-4675-BA15-0000A458946C']
    length_pred = 60
    cycle_len = 30

    clt, surv_rate = get_estimate_clt_and_surv_rate_by_customers(customer_id_list, length_pred, cycle_len)
    """
    subscriptions_df = self.get_subscriptions_of_customers(series_of_customer_ids)
    subscriptions_df = self.add_billing_frequency(subscriptions_df)
    customers_df = self.get_data_on_customer_level(subscriptions_df, cycle_length)
    min_cohort = np.min(customers_df['cohort'])

    clt_table = pd.DataFrame([])

    active_lost_df = self.active_subs(customers_df)
    start_subs_cohort = active_lost_df['active'][0]
    max_cycles = np.round((date.today()-date(int(min_cohort[:4]), int(min_cohort[5:]), 1)).days/cycle_length)
    active_lost_df = active_lost_df.loc[active_lost_df['billing_cycles']<=max_cycles]

    if not active_lost_df['active'].tolist():
      print("This cohort do not have billing cycles")
      return 

    t_max = max(active_lost_df['billing_cycles'])
    if t_max < 2:
      print("This cohort do not have enough billing cycles")
      return

    # general dataframe for cohort
    all_cycles = range(0, length_prediction+1)
    survival_rate_table = pd.DataFrame(data={'cycles': list(all_cycles)})
    survival_rate_table = pd.merge(survival_rate_table, active_lost_df, left_on=['cycles'], right_on=['billing_cycles'], how='left')

    # CLT calculated with simple moving average
    rolling_window_churn = self.simple_moving_average(active_lost_df['actual_churn'], length_prediction+1)
    survival_rate_table['predicted_churn_linear'] = rolling_window_churn
    survival_rate_table['predicted_survival_linear'] = np.append([1.0], np.cumprod(1-survival_rate_table['predicted_churn_linear']).to_list()[1:])
    CLT_linear = np.sum(survival_rate_table['predicted_survival_linear'])

    # CLT calculated with fader hardie
    alpha, beta = self.minimiza_alpha_beta(active_lost_df['active'].to_list()[1:], active_lost_df['lost'].to_list()[1:])
    CLT_fader, survival_fader = self.e_clt(alpha, beta, length_prediction)
    survival_rate_table['predicted_survival_fader'] = survival_fader

    # mae
    y_true = survival_rate_table.dropna()['actual_survival']
    y_pred = survival_rate_table.dropna()['predicted_survival_fader']
    mae = np.sum(np.abs(y_true-y_pred))/len(y_true)

    clt_table = pd.DataFrame(data={'size': [start_subs_cohort],
                                    'clt_linear': [CLT_linear],
                                    'clt_fader': [CLT_fader],
                                    'mae_fader': [mae],
                                    't_max': [t_max]})

    return clt_table, survival_rate_table
