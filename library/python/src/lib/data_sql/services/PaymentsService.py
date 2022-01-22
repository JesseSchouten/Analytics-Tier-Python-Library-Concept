from pyspark.sql import DataFrame


class PaymentsService:
    def __init__(self, context):
        self.context = context
        self.table_name = "payments"
        self.columns = ['psp_reference', 'merchant_reference', 'merchant_account', 'channel', 'country',
                        'creation_date', 'gross_currency', 'gross_debit',
                        'gross_credit', 'gross_received', 'net_currency', 'net_debit', 'net_credit',
                        'net_received', 'exchange_rate', 'commission', 'markup', 'scheme_fees', 'interchange',
                        'total_fees', 'suspect_fraud', 'has_chargeback', 'has_reversed_chargeback',
                        'has_refund', 'has_reversed_refund', 'cb_is_voluntary', 'cb_not_voluntary']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['PaymentsRepository']

        return repository.insert(data, batch_size)
