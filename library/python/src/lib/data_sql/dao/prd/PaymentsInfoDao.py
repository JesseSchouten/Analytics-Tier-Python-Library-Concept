class PaymentsInfoDao:
    def __init__(self, context):
        self.context = context

    def get_insert_query(self):
        return None

    def get_update_query(self):
        return None

    def get_upsert_query(self):
        return """ 
        MERGE payments_info AS TARGET  
        USING payments_info_temp AS SOURCE 
        ON SOURCE.modification_reference = TARGET.modification_reference
            AND SOURCE.type = TARGET.type
            AND SOURCE.creation_date = TARGET.creation_date
            AND SOURCE.batch_number = TARGET.batch_number
        WHEN MATCHED THEN
            UPDATE SET 
                TARGET.company_account = SOURCE.company_account, 
                TARGET.merchant_account = SOURCE.merchant_account,
                TARGET.psp_reference = SOURCE.psp_reference,
                TARGET.merchant_reference = SOURCE.merchant_reference, 
                TARGET.payment_method = SOURCE.payment_method, 
                TARGET.time_zone = SOURCE.time_zone, 
                TARGET.booking_date = SOURCE.booking_date,
                TARGET.booking_date_time_zone = SOURCE.booking_date_time_zone, 
                TARGET.gross_currency = SOURCE.gross_currency, 
                TARGET.gross_debit = SOURCE.gross_debit,
                TARGET.gross_credit = SOURCE.gross_credit, 
                TARGET.exchange_rate = SOURCE.exchange_rate,
                TARGET.net_currency = SOURCE.net_currency,
                TARGET.net_debit = SOURCE.net_debit,
                TARGET.net_credit= SOURCE.net_credit, 
                TARGET.commission = SOURCE.commission, 
                TARGET.markup = SOURCE.markup, 
                TARGET.scheme_fees = SOURCE.scheme_fees,
                TARGET.interchange = SOURCE.interchange, 
                TARGET.payment_method_variant = SOURCE.payment_method_variant, 
                TARGET.payment_received_date = SOURCE.payment_received_date,
                TARGET.afterpay = SOURCE.afterpay
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                company_account, 
                merchant_account,
                psp_reference, 
                merchant_reference, 
                payment_method, 
                creation_date, 
                time_zone, 
                booking_date,
                booking_date_time_zone, 
                type,
                modification_reference, 
                gross_currency, 
                gross_debit,
                gross_credit, 
                exchange_rate,
                net_currency,
                net_debit,
                net_credit, 
                commission, 
                markup, 
                scheme_fees,
                interchange, 
                payment_method_variant, 
                batch_number, 
                payment_received_date,
                afterpay) 
        VALUES (
                SOURCE.company_account, 
                SOURCE.merchant_account,
                SOURCE.psp_reference, 
                SOURCE.merchant_reference, 
                SOURCE.payment_method, 
                SOURCE.creation_date, 
                SOURCE.time_zone, 
                SOURCE.booking_date,
                SOURCE.booking_date_time_zone, 
                SOURCE.type,
                SOURCE.modification_reference, 
                SOURCE.gross_currency, 
                SOURCE.gross_debit,
                SOURCE.gross_credit, 
                SOURCE.exchange_rate,
                SOURCE.net_currency,
                SOURCE.net_debit,
                SOURCE.net_credit, 
                SOURCE.commission, 
                SOURCE.markup, 
                SOURCE.scheme_fees,
                SOURCE.interchange, 
                SOURCE.payment_method_variant, 
                SOURCE.batch_number, 
                SOURCE.payment_received_date,
                SOURCE.afterpay);
        """

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[payments_info_temp]"

        # Insert spark df into a temporary table
        data.write \
            .format('sql') \
            .jdbc(url=sql_connector.jdbc_url, table=table, mode='overwrite')

        sql_connector = self.context['SQLConnector']
        sql_connector.open_connection(conn_type='pymssql')
        cursor = sql_connector.conn.cursor()

        # Merge temp table to main table, kind of in an upsert fashion
        try:
            cursor.execute(self.get_upsert_query())
        except Exception as e:
            raise Exception(e)
        finally:
            #Remove temp table
            delete_query = f"DROP TABLE {table}"
            cursor.execute(delete_query)

            sql_connector.conn.commit()
            sql_connector.close_connection()
        return True
