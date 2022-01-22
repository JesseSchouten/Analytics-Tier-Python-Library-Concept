class PaymentsDao:
    def __init__(self, context):
        self.context = context

    def get_insert_query(self):
        return 'INSERT INTO [dbo].[payments] ' \
               '(psp_reference, merchant_reference, merchant_account, channel, country, creation_date, ' \
               'gross_currency, gross_debit, gross_credit, commission, markup, scheme_fees, interchange, total_fees, ' \
               'gross_received, net_currency, net_debit, net_credit, net_received,' \
               'exchange_rate, suspect_fraud, has_chargeback, has_reversed_chargeback, has_refund, ' \
               'has_reversed_refund, cb_is_voluntary, cb_not_voluntary)' \
               ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?)'

    def get_update_query(self):
        return 'UPDATE [dbo].[payments] ' \
               'SET merchant_reference = ?, merchant_account = ?, creation_date = ?, channel = ?, country = ?,' \
               'gross_currency = ?, gross_debit = ?, gross_credit = ?, gross_received = ?, ' \
               'net_currency = ?, net_debit = ?, net_credit = ?, net_received = ?, exchange_rate = ?,' \
               'commission = ?, markup = ?, scheme_fees = ?, interchange = ?, total_fees = ?,' \
               'suspect_fraud = ?, has_chargeback = ?, has_reversed_chargeback = ?, ' \
               'has_refund = ?, has_reversed_refund = ?, ' \
               'cb_is_voluntary = ?, cb_not_voluntary = ?, updated = CURRENT_TIMESTAMP ' \
               'WHERE psp_reference = ?'

    def get_upsert_query(self):
        return """ 
        MERGE payments AS TARGET  
        USING payments_temp AS SOURCE 
        ON (SOURCE.psp_reference = TARGET.psp_reference)
        WHEN MATCHED THEN
            UPDATE SET 
                TARGET.merchant_reference = SOURCE.merchant_reference,
                TARGET.merchant_account = SOURCE.merchant_account, 
                TARGET.channel = SOURCE.channel, 
                TARGET.country = SOURCE.country, 
                TARGET.creation_date = SOURCE.creation_date, 
                TARGET.gross_currency = SOURCE.gross_currency, 
                TARGET.gross_debit = SOURCE.gross_debit, 
                TARGET.gross_credit = SOURCE.gross_credit, 
                TARGET.gross_received = SOURCE.gross_received, 
                TARGET.net_currency = SOURCE.net_currency, 
                TARGET.net_debit = SOURCE.net_debit, 
                TARGET.net_credit = SOURCE.net_credit, 
                TARGET.net_received = SOURCE.net_received, 
                TARGET.exchange_rate = SOURCE.exchange_rate, 
                TARGET.commission = SOURCE.commission, 
                TARGET.markup = SOURCE.markup, 
                TARGET.scheme_fees = SOURCE.scheme_fees, 
                TARGET.interchange = SOURCE.interchange, 
                TARGET.total_fees = SOURCE.total_fees, 
                TARGET.suspect_fraud = SOURCE.suspect_fraud, 
                TARGET.has_chargeback = SOURCE.has_chargeback, 
                TARGET.has_reversed_chargeback = SOURCE.has_reversed_chargeback, 
                TARGET.has_refund = SOURCE.has_refund, 
                TARGET.has_reversed_refund = SOURCE.has_reversed_refund,
                TARGET.cb_is_voluntary = SOURCE.cb_is_voluntary, 
                TARGET.cb_not_voluntary = SOURCE.cb_not_voluntary 
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                psp_reference, 
                merchant_reference,
                merchant_account, 
                channel, 
                country, 
                creation_date, 
                gross_currency, 
                gross_debit,
                gross_credit, 
                gross_received,
                net_currency, 
                net_debit, 
                net_credit, 
                net_received,
                exchange_rate,
                commission,
                markup, 
                scheme_fees, 
                interchange, 
                total_fees,
                suspect_fraud, 
                has_chargeback, 
                has_reversed_chargeback, 
                has_refund, 
                has_reversed_refund,
                cb_is_voluntary, 
                cb_not_voluntary) 
        VALUES (
            SOURCE.psp_reference, 
            SOURCE.merchant_reference, 
            SOURCE.merchant_account,
            SOURCE.channel, 
            SOURCE.country, 
            SOURCE.creation_date, 
            SOURCE.gross_currency, 
            SOURCE.gross_debit,
            SOURCE.gross_credit, 
            SOURCE.gross_received, 
            SOURCE.net_currency, 
            SOURCE.net_debit, 
            SOURCE.net_credit, 
            SOURCE.net_received, 
            SOURCE.exchange_rate, 
            SOURCE.commission, 
            SOURCE.markup, 
            SOURCE.scheme_fees, 
            SOURCE.interchange, 
            SOURCE.total_fees,
            SOURCE.suspect_fraud, 
            SOURCE.has_chargeback, 
            SOURCE.has_reversed_chargeback, 
            SOURCE.has_refund, 
            SOURCE.has_reversed_refund, 
            SOURCE.cb_is_voluntary, 
            SOURCE.cb_not_voluntary
        );
        """

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[payments_temp]"

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
