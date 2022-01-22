from pyspark.sql import DataFrame
import logging


class WicketlabsCustomersService:
    def __init__(self, context):
        self.context = context
        self.table_name = "wicketlabs_customers"
        self.columns = ['customer_id', 'subscription_Id', 'active_start_date',
                        'trial_start_date', 'active_end_date', 'channel', 'product_name',
                        'is_reconnect', 'used_special_offer', 'subscription_unit',
                        'unit_price', 'unit_price_currency', 'total_payments',
                        'last_view_date', 'days_since_last_engagement', 'tenure',
                        'engagement_status', 'primary_device_life_time', 'primary_device_last_three_months',
                        'primary_viewing_location_life_time', 'primary_viewing_location_last_three_months',
                        'hours_viewed_last_three_months', 'customer_subscription_count', 'customer_tenure', 'customer_voluntary_cancellation_count',
                        'customer_hours_viewed', 'entertainment_index_last_three_months', 'entertainment_index_lifetime',
                        'date', 'id']
        self.data_type = DataFrame

    def run_insert(self, data):
        self.validate(data)

        repository = self.context['WicketlabsCustomersRepository']

        return repository.insert(data)

    def validate(self, data):
        if not isinstance(data, self.data_type):
            logging.warn('incorrect data type. Expecting a sprak DataFrame')
            raise ValueError('Spark dataframe required')
        if len(data.columns) != len(self.columns):
            logging.warn('incorrect data input.')
            raise ValueError('incorrect column lengt')
