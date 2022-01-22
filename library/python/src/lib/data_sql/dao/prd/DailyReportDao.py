class DailyReportDao:
    def __init__(self, context):
        self.context = context

    def get_insert_query(self):
        return None

    def get_update_query(self):
        return None

    def get_upsert_query(self):
        return """
        MERGE daily_report AS TARGET  
        USING daily_report_temp AS SOURCE 
        ON SOURCE.date = TARGET.date
            AND SOURCE.channel = TARGET.channel
            AND SOURCE.country = TARGET.country
        WHEN MATCHED THEN
            UPDATE SET 
                TARGET.nr_trials = SOURCE.nr_trials,
                TARGET.nr_reconnects = SOURCE.nr_reconnects,
                TARGET.nr_paid_customers = SOURCE.nr_paid_customers,
                TARGET.nr_paid_customers_subscriptions = SOURCE.nr_paid_customers_subscriptions,
                TARGET.nr_active_customers = SOURCE.nr_active_customers,
                TARGET.nr_active_subscriptions = SOURCE.nr_active_subscriptions,
                TARGET.nr_payments = SOURCE.nr_payments,
                TARGET.nr_chargebacks = SOURCE.nr_chargebacks,
                TARGET.nr_reversed_chargebacks = SOURCE.nr_reversed_chargebacks,
                TARGET.nr_refunds = SOURCE.nr_refunds,    
                TARGET.nr_reversed_refunds = SOURCE.nr_reversed_refunds, 
                TARGET.net_currency = SOURCE.net_currency, 
                TARGET.net_debit = SOURCE.net_debit, 
                TARGET.net_credit = SOURCE.net_credit, 
                TARGET.net_received = SOURCE.net_received, 
                TARGET.commission = SOURCE.commission, 
                TARGET.markup = SOURCE.markup, 
                TARGET.scheme_fees = SOURCE.scheme_fees, 
                TARGET.interchange = SOURCE.interchange, 
                TARGET.total_fees = SOURCE.total_fees,
                TARGET.sum_of_gross_sales = SOURCE.sum_of_gross_sales,
                TARGET.sum_of_refund_amount = SOURCE.sum_of_refund_amount,
                TARGET.sum_of_refund_reversed = SOURCE.sum_of_refund_reversed,
                TARGET.sum_of_fee = SOURCE.sum_of_fee,
                TARGET.sum_of_refund_fee = SOURCE.sum_of_refund_fee,
                TARGET.sum_of_transaction_costs = SOURCE.sum_of_transaction_costs,
                TARGET.sum_of_chargeback_fee = SOURCE.sum_of_chargeback_fee,
                TARGET.sum_of_chargeback = SOURCE.sum_of_chargeback,
                TARGET.sum_of_chargeback_reversed = SOURCE.sum_of_chargeback_reversed,
                TARGET.sum_of_invoice_deduction = SOURCE.sum_of_invoice_deduction,
                TARGET.sum_of_balance_transfer = SOURCE.sum_of_balance_transfer,
                TARGET.sum_of_deposit_correction = SOURCE.sum_of_deposit_correction,
                TARGET.sum_of_net_payout = SOURCE.sum_of_net_payout             
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                date,
                channel,
                country,
                nr_trials,
                nr_reconnects,
                nr_paid_customers,
                nr_paid_customers_subscriptions,
                nr_active_customers,
                nr_active_subscriptions,
                nr_payments,
                nr_chargebacks,
                nr_reversed_chargebacks,
                nr_refunds,
                nr_reversed_refunds,
                net_currency,
                net_debit,
                net_credit,
                net_received,
                commission,
                markup,
                scheme_fees,
                interchange,
                total_fees,
                sum_of_gross_sales,
                sum_of_refund_amount,
                sum_of_refund_reversed,
                sum_of_fee,
                sum_of_refund_fee,
                sum_of_transaction_costs,
                sum_of_chargeback_fee,
                sum_of_chargeback,
                sum_of_chargeback_reversed,
                sum_of_invoice_deduction,
                sum_of_balance_transfer,
                sum_of_deposit_correction,
                sum_of_net_payout
            ) 
        VALUES (
            SOURCE.date,
            SOURCE.channel,
            SOURCE.country,
            SOURCE.nr_trials,
            SOURCE.nr_reconnects,
            SOURCE.nr_paid_customers,
            SOURCE.nr_paid_customers_subscriptions,
            SOURCE.nr_active_customers,
            SOURCE.nr_active_subscriptions,
            SOURCE.nr_payments,
            SOURCE.nr_chargebacks,
            SOURCE.nr_reversed_chargebacks,
            SOURCE.nr_refunds,
            SOURCE.nr_reversed_refunds,
            SOURCE.net_currency,
            SOURCE.net_debit,
            SOURCE.net_credit,
            SOURCE.net_received,
            SOURCE.commission,
            SOURCE.markup,
            SOURCE.scheme_fees,
            SOURCE.interchange,
            SOURCE.total_fees,
            SOURCE.sum_of_gross_sales,
            SOURCE.sum_of_refund_amount,
            SOURCE.sum_of_refund_reversed,
            SOURCE.sum_of_fee,
            SOURCE.sum_of_refund_fee,
            SOURCE.sum_of_transaction_costs,
            SOURCE.sum_of_chargeback_fee,
            SOURCE.sum_of_chargeback,
            SOURCE.sum_of_chargeback_reversed,
            SOURCE.sum_of_invoice_deduction,
            SOURCE.sum_of_balance_transfer,
            SOURCE.sum_of_deposit_correction,
            SOURCE.sum_of_net_payout            
        );
        """

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[daily_report_temp]"

        # Insert spark df into a temporary table
        data.write \
            .format('sql') \
            .jdbc(url=sql_connector.jdbc_url, table=table, mode='overwrite')

        sql_connector = self.context['SQLConnector']
        sql_connector.open_connection(conn_type='pymssql')
        cursor = sql_connector.conn.cursor()

        # Merge temp table to main table, kind of in an upsert fashion
        cursor.execute(self.get_upsert_query())

        # Remove temp table
        delete_query = f"DROP TABLE {table}"
        cursor.execute(delete_query)

        sql_connector.conn.commit()
        sql_connector.close_connection()
        return True
