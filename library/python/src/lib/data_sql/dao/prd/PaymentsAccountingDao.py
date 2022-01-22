class PaymentsAccountingDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """
        MERGE payments_accounting AS TARGET  
        USING payments_accounting_temp AS SOURCE 
        ON (SOURCE.psp_reference = TARGET.psp_reference AND
            SOURCE.booking_date = TARGET.booking_date AND
            SOURCE.record_type = TARGET.record_type AND
            SOURCE.processing_fee_fc = TARGET.processing_fee_fc)
        WHEN MATCHED THEN
            UPDATE SET 
                TARGET.company_account = SOURCE.company_account,
                TARGET.merchant_account = SOURCE.merchant_account,
                TARGET.merchant_reference = SOURCE.merchant_reference,
                TARGET.payment_method = SOURCE.payment_method,
                TARGET.time_zone = SOURCE.time_zone,
                TARGET.main_currency = SOURCE.main_currency,
                TARGET.main_amount = SOURCE.main_amount,
                TARGET.payment_currency = SOURCE.payment_currency,
                TARGET.received_pc = SOURCE.received_pc,
                TARGET.authorized_pc = SOURCE.authorized_pc,
                TARGET.captured_pc = SOURCE.captured_pc,    
                TARGET.settlement_currency = SOURCE.settlement_currency, 
                TARGET.payable_sc = SOURCE.payable_sc, 
                TARGET.commission_sc = SOURCE.commission_sc, 
                TARGET.markup_sc = SOURCE.markup_sc, 
                TARGET.scheme_fees_sc = SOURCE.scheme_fees_sc, 
                TARGET.interchange_sc = SOURCE.interchange_sc, 
                TARGET.processing_fee_currency = SOURCE.processing_fee_currency, 
                TARGET.payment_method_variant = SOURCE.payment_method_variant          
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                company_account,
                merchant_account, 
                psp_reference, 
                merchant_reference,
                payment_method, 
                booking_date, 
                time_zone, 
                main_currency,
                main_amount, 
                record_type, 
                payment_currency, 
                received_pc,
                authorized_pc, 
                captured_pc, 
                settlement_currency, 
                payable_sc,
                commission_sc, 
                markup_sc, 
                scheme_fees_sc, 
                interchange_sc,
                processing_fee_currency, 
                processing_fee_fc, 
                payment_method_variant
            ) 
        VALUES (
                SOURCE.company_account,
                SOURCE.merchant_account, 
                SOURCE.psp_reference, 
                SOURCE.merchant_reference,
                SOURCE.payment_method, 
                SOURCE.booking_date, 
                SOURCE.time_zone, 
                SOURCE.main_currency,
                SOURCE.main_amount, 
                SOURCE.record_type, 
                SOURCE.payment_currency, 
                SOURCE.received_pc,
                SOURCE.authorized_pc, 
                SOURCE.captured_pc, 
                SOURCE.settlement_currency, 
                SOURCE.payable_sc,
                SOURCE.commission_sc, 
                SOURCE.markup_sc, 
                SOURCE.scheme_fees_sc, 
                SOURCE.interchange_sc,
                SOURCE.processing_fee_currency, 
                SOURCE.processing_fee_fc, 
                SOURCE.payment_method_variant         
        );
        """

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[payments_accounting_temp]"

        # Insert spark df into a temporary table
        data.write \
            .format('sql') \
            .jdbc(url=sql_connector.jdbc_url, table=table, mode='overwrite')

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
