class SubscriptionPlanService:
    def __init__(self, context):
        self.context = context
        self.table_name = "subscription_plan"
        self.columns = ['id', 'channel', 'country', 'pause_max_days', 'system',
                        'free_trial', 'billing_cycle_type', 'description', 'price',
                        'asset_type', 'only_available_with_promotion', 'billing_frequency',
                        'recurring', 'catch_up_hours', 'pause_min_days', 'number_of_supported_devices',
                        'currency', 'business_type', 'is_downloadable', 'only_available_for_logged_in_users',
                        'title', 'start', 'end', 'subscription_plan_type']
        self.data_type = list

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data[0]) != len(self.columns):
            return False

        repository = self.context['SubscriptionPlanRepository']

        return repository.insert(data, batch_size)
