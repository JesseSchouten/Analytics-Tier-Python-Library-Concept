import pandas as pd


class ViewingService:
    def __init__(self, context):
        self.context = context
        self.table_name = "viewing"
        self.columns = ['timestamp', 'channel', 'subscription_plan_id', 'video_id', 'video_type', 'platform',
                        'video_playtime_minutes']
        self.data_type = pd.DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False
        elif set(data.columns) != set(self.columns):
            return False

        data = data[['timestamp', 'channel', 'subscription_plan_id', 'video_id', 'video_type', 'platform',
                     'video_playtime_minutes']]\
            .groupby(
            ['timestamp', 'channel', 'subscription_plan_id', 'video_id', 'video_type', 'platform'],
            dropna=False)\
            .sum()\
            .reset_index()

        repository = self.context['ViewingRepository']

        return repository.insert(data, batch_size)
