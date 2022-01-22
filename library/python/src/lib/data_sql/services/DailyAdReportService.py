from pyspark.sql import DataFrame


class DailyAdReportService:
    def __init__(self, context):
        self.context = context
        self.table_name = "daily_ad_report"
        self.columns = ['date', 'channel', 'country', 'social_channel', 'account_id',
                        'account_name', 'campaign_id', 'campaign_name', 'adset_id',
                        'adset_name', 'ad_id', 'ad_name', 'time_zone', 'currency', 
                        'paid_impressions', 'paid_clicks', 'costs']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['DailyAdReportRepository']

        return repository.insert(data, batch_size)