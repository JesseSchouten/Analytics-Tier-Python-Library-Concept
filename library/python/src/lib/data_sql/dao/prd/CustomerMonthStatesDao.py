class CustomerMonthStatesDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """MERGE customer_month_states AS TARGET USING customer_month_states_temp AS SOURCE
        ON SOURCE.month = TARGET.month AND SOURCE.customer_id = TARGET.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                TARGET.channel = SOURCE.channel,
                TARGET.cohort = SOURCE.cohort,
                TARGET.age = SOURCE.age,
                TARGET.new_customer = SOURCE.new_customer,
                TARGET.is_active = SOURCE.is_active,
                TARGET.has_new_subscription = SOURCE.has_new_subscription,
                TARGET.is_trialist = SOURCE.is_trialist,
                TARGET.is_reconnect = SOURCE.is_reconnect,
                TARGET.currency = SOURCE.currency,
                TARGET.gross_debit = SOURCE.gross_debit,
                TARGET.gross_credit = SOURCE.gross_credit,
                TARGET.gross_received = SOURCE.gross_received,
                TARGET.net_debit = SOURCE.net_debit,
                TARGET.net_credit = SOURCE.net_credit,
                TARGET.net_received = SOURCE.net_received,
                TARGET.commission = SOURCE.commission,
                TARGET.markup = SOURCE.markup,
                TARGET.scheme_fees = SOURCE.scheme_fees,
                TARGET.interchange = SOURCE.interchange,
                TARGET.total_fees = SOURCE.total_fees,
                TARGET.is_paying = SOURCE.is_paying,
                TARGET.gross_debit_total = SOURCE.gross_debit_total,
                TARGET.gross_credit_total = SOURCE.gross_credit_total,
                TARGET.gross_received_total = SOURCE.gross_received_total,
                TARGET.net_debit_total = SOURCE.net_debit_total,
                TARGET.net_credit_total = SOURCE.net_credit_total,
                TARGET.net_received_total = SOURCE.net_received_total,
                TARGET.commission_total = SOURCE.commission_total,
                TARGET.markup_total = SOURCE.markup_total,
                TARGET.scheme_fees_total = SOURCE.scheme_fees_total,
                TARGET.interchange_total = SOURCE.interchange_total,
                TARGET.total_fees_total = SOURCE.total_fees_total,
                TARGET.has_chargeback = SOURCE.has_chargeback,
                TARGET.has_reversed_chargeback = SOURCE.has_reversed_chargeback,
                TARGET.has_refund = SOURCE.has_refund
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                customer_id,
                month,
                channel,
                cohort,
                age,
                new_customer,
                is_active,
                has_new_subscription,
                is_trialist,
                is_reconnect,
                currency,
                gross_debit,
                gross_credit,
                gross_received,
                net_debit,
                net_credit,
                net_received,
                commission,
                markup,
                scheme_fees,
                interchange,
                total_fees,
                is_paying,
                gross_debit_total,
                gross_credit_total,
                gross_received_total,
                net_debit_total,
                net_credit_total,
                net_received_total,
                commission_total,
                markup_total,
                scheme_fees_total,
                interchange_total,
                total_fees_total,
                has_chargeback,
                has_reversed_chargeback,
                has_refund
            ) VALUES (
                SOURCE.customer_id,
                SOURCE.month,
                SOURCE.channel,
                SOURCE.cohort,
                SOURCE.age,
                SOURCE.new_customer,
                SOURCE.is_active,
                SOURCE.has_new_subscription,
                SOURCE.is_trialist,
                SOURCE.is_reconnect,
                SOURCE.currency,
                SOURCE.gross_debit,
                SOURCE.gross_credit,
                SOURCE.gross_received,
                SOURCE.net_debit,
                SOURCE.net_credit,
                SOURCE.net_received,
                SOURCE.commission,
                SOURCE.markup,
                SOURCE.scheme_fees,
                SOURCE.interchange,
                SOURCE.total_fees,
                SOURCE.is_paying,
                SOURCE.gross_debit_total,
                SOURCE.gross_credit_total,
                SOURCE.gross_received_total,
                SOURCE.net_debit_total,
                SOURCE.net_credit_total,
                SOURCE.net_received_total,
                SOURCE.commission_total,
                SOURCE.markup_total,
                SOURCE.scheme_fees_total,
                SOURCE.interchange_total,
                SOURCE.total_fees_total,
                SOURCE.has_chargeback,
                SOURCE.has_reversed_chargeback,
                SOURCE.has_refund
            );"""

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[customer_month_states_temp]"

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
