from pyspark.sql import DataFrame
import logging


class MonthReportService:
    def __init__(self, context):
        self.context = context
        self.table_name = "month_report"
        self.columns = [
            'month',
            'channel',
            'country',
            'nr_trials',
            'nr_reconnects',
            'nr_unattributed_new_subs',
            'nr_paid_customers',
            'nr_paid_customers_subscriptions',
            'nr_active_customers',
            'nr_active_subscriptions',
            'nr_active_subscriptions_next_month',
            'active_subscription_churn_rate',
            'nr_paid_subscriptions',
            'nr_paid_subscriptions_next_month',
            'paid_subscription_churn_rate',
            'nr_payments',
            'nr_chargebacks',
            'nr_reversed_chargebacks',
            'nr_refunds',
            'nr_reversed_refunds',
            'net_currency',
            'net_debit',
            'net_credit',
            'net_received',
            'commission',
            'markup',
            'scheme_fees',
            'interchange',
            'total_fees',
            'sum_of_gross_sales',
            'sum_of_refund_amount',
            'sum_of_refund_reversed',
            'sum_of_fee',
            'sum_of_refund_fee',
            'sum_of_transaction_costs',
            'sum_of_chargeback_fee',
            'sum_of_chargeback',
            'sum_of_chargeback_reversed',
            'sum_of_invoice_deduction',
            'sum_of_balance_transfer',
            'sum_of_deposit_correction',
            'sum_of_net_payout']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        self.validate(data)

        repository = self.context['MonthReportRepository']

        return repository.insert(data, batch_size)

    def validate(self, data):
        if not isinstance(data, self.data_type):
            logging.warn('incorrect data type. Expecting a sprak DataFrame')
            raise ValueError('Spark dataframe required')
        if len(data.columns) != len(self.columns):
            logging.warn('incorrect data input.')
            raise ValueError('incorrect column lengt')
