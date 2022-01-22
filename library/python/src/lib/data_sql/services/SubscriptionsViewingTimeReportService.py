class SubscriptionsViewingTimeReportService:
    def __init__(self, context):
        self.context = context
        self.table_name = "subscriptions_viewing_time_report"
        self.columns = ['id', 'channel', 'customer_id', 'customer_registration_country',
                        'subscription_created_date', 'subscription_start_date', 'subscription_plan_id',
                        'subscription_plan_billingcycle_type', 'subscription_plan_billingfrequency',
                        'subscription_plan_price', 'subscription_plan_currency']
        self.data_type = list

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data[0]) != len(self.columns):
            return False

        repository = self.context['SubscriptionsViewingTimeReportRepository']

        return repository.insert(data, batch_size)
