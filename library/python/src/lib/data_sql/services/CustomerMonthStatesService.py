from pyspark.sql import DataFrame


class CustomerMonthStatesService:
    def __init__(self, context):
        self.context = context
        self.table_name = "customer_month_states"
        self.columns = ['customer_id', 'month', 'channel', 'cohort', 'age', 'new_customer', 'is_active',
                        'has_new_subscription', 'is_trialist', 'is_reconnect', 'currency', 'gross_debit',
                        'gross_credit', 'gross_received', 'net_debit', 'net_credit', 'net_received', 'commission',
                        'markup', 'scheme_fees', 'interchange', 'total_fees', 'is_paying', 'gross_debit_total',
                        'gross_credit_total', 'gross_received_total', 'net_debit_total', 'net_credit_total',
                        'net_received_total', 'commission_total', 'markup_total', 'scheme_fees_total',
                        'interchange_total', 'total_fees_total', 'has_chargeback', 'has_reversed_chargeback',
                        'has_refund']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['CustomerMonthStatesRepository']

        return repository.insert(data, batch_size)
