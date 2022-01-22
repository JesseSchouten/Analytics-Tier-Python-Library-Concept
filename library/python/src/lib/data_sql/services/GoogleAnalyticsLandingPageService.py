from pyspark.sql import DataFrame


class GoogleAnalyticsLandingPageService:
    def __init__(self, context):
        self.context = context
        self.table_name = "daily_ga_landing_page_report"
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
            'landing',
            'register',
            'choose_plan',
            'payment',
            'succes'
        ]

    def run_insert(self, data):
        self.validate(data)

        repository = self.context['GoogleAnalyticsLandingPageRepository']

        return repository.insert(data)

    def validate(self, data):
        if not isinstance(data, self.data_type):
            print('incorrect data type. Expecting a sprak DataFrame')
            raise ValueError('Spark dataframe required')
        if len(data.columns) != len(self.columns):
            print('incorrect data input.')
            raise ValueError('incorrect column lengt')
