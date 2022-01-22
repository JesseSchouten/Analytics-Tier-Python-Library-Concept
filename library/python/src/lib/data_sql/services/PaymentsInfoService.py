from pyspark.sql import DataFrame


class PaymentsInfoService:
    def __init__(self, context):
        self.context = context
        self.table_name = "payments_info"
        self.columns = ['company_account',
                        'merchant_account',
                        'psp_reference',
                        'merchant_reference',
                        'payment_method',
                        'creation_date',
                        'time_zone',
                        'booking_date',
                        'booking_date_time_zone',
                        'type',
                        'modification_reference',
                        'gross_currency',
                        'gross_debit',
                        'gross_credit',
                        'exchange_rate',
                        'net_currency',
                        'net_debit',
                        'net_credit',
                        'commission',
                        'markup',
                        'scheme_fees',
                        'interchange',
                        'payment_method_variant',
                        'batch_number',
                        'payment_received_date',
                        'afterpay']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['PaymentsInfoRepository']

        return repository.insert(data, batch_size)
