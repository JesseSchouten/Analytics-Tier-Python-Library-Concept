from pyspark.sql import DataFrame
import logging


class GoogleAnalyticsDailyGoalsService:
    def __init__(self, context):
        self.context = context
        self.table_name = "daily_ga_goals_report"
        self.data_type = DataFrame
        self.columns = [
            'date',
            'account_id',
            'property_id',
            'property_name',
            'view_id',
            'view_name',
            'view_timezone',
            'channel',
            'country',
            'segment',
            'source',
            'medium',
            'campaign_name',
            'ad_name',
            'sessions',
            'new_users',
            'pageviews',
            'bounces',
            'goal_1_completions',
            'goal_2_completions',
            'goal_3_completions',
            'goal_4_completions',
            'goal_5_completions',
            'goal_6_completions',
            'goal_7_completions',
            'goal_8_completions',
            'goal_9_completions',
            'goal_10_completions',
            'goal_11_completions',
            'goal_12_completions',
            'goal_13_completions',
            'goal_14_completions',
            'goal_15_completions',
            'goal_16_completions',
            'goal_17_completions',
            'goal_18_completions',
            'goal_19_completions',
            'goal_20_completions'
        ]

    def run_insert(self, data):
        self.validate(data)

        repository = self.context['GoogleAnalyticsDailyGoalsRepository']

        return repository.insert(data)

    def validate(self, data):
        if not isinstance(data, self.data_type):
            logging.warn('incorrect data type. Expecting a sprak DataFrame')
            raise ValueError('Spark dataframe required')
        if len(data.columns) != len(self.columns):
            logging.warn('incorrect data input.')
            raise ValueError('incorrect column lengt')
