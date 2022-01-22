from pyspark.sql import DataFrame


class DailyReportService:
    def __init__(self, context):
        self.context = context
        self.table_name = "daily_report"
        self.columns = ['date', 'channel', 'country', 'nr_trials', 'nr_reconnects', 'nr_paid_customers',
                        'nr_paid_customers_subscriptions', 'nr_active_customers', 'nr_active_subscriptions',
                        'nr_payments', 'nr_chargebacks',
                        'nr_reversed_chargebacks', 'nr_refunds', 'nr_reversed_refunds', 'net_currency',
                        'net_debit','net_credit', 'net_received', 'commission', 'markup',
                        'scheme_fees', 'interchange', 'total_fees',
                        'sum_of_gross_sales', 'sum_of_refund_amount', 'sum_of_refund_reversed', 'sum_of_fee',
                        'sum_of_refund_fee', 'sum_of_transaction_costs',
                        'sum_of_chargeback_fee', 'sum_of_chargeback', 'sum_of_chargeback_reversed',
                        'sum_of_invoice_deduction', 'sum_of_balance_transfer', 'sum_of_deposit_correction',
                        'sum_of_net_payout']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['DailyReportRepository']

        return repository.insert(data, batch_size)
