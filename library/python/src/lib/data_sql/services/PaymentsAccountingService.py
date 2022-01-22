from pyspark.sql import DataFrame


class PaymentsAccountingService:
    def __init__(self, context):
        self.context = context
        self.table_name = "payments_accounting"
        self.columns = ['company_account', 'merchant_account', 'psp_reference', 'merchant_reference',
                        'payment_method', 'booking_date', 'time_zone', 'main_currency',
                        'main_amount', 'record_type', 'payment_currency', 'received_pc',
                        'authorized_pc', 'captured_pc', 'settlement_currency', 'payable_sc',
                        'commission_sc', 'markup_sc', 'scheme_fees_sc', 'interchange_sc',
                        'processing_fee_currency', 'processing_fee_fc', 'payment_method_variant'
                        ]
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['PaymentsAccountingRepository']

        return repository.insert(data, batch_size)