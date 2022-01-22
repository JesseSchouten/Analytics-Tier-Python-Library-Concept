from pyspark.sql import DataFrame

class SubscriptionsService:
    def __init__(self, context):
        self.context = context
        self.table_name = "subscriptions"
        self.columns = ['subscription_id',
                        'customer_id',
                        'channel',
                        'country',
                        'subscription_plan_id',
                        'subscription_create_date',
                        'subscription_start',
                        'subscription_end',
                        'subscription_paused_date',
                        'subscription_resumable_days',
                        'state',
                        'recurring_enabled',
                        'payment_provider',
                        'free_trial',
                        'notes',
                        'is_pausable',
                        'is_resumable',
                        'new_plan_id',
                        'first_payment_date']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['SubscriptionsRepository']

        return repository.insert(data, batch_size)
