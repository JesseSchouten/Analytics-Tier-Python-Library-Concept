class PaymentsReceivedDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """
        MERGE payments_received AS TARGET  
        USING payments_received_temp AS SOURCE 
        ON (SOURCE.psp_reference = TARGET.psp_reference)
        WHEN MATCHED THEN
            UPDATE SET 
                TARGET.company_account = SOURCE.company_account,
                TARGET.merchant_account = SOURCE.merchant_account,
                TARGET.merchant_reference = SOURCE.merchant_reference,
                TARGET.payment_method = SOURCE.payment_method,
                TARGET.creation_date = SOURCE.creation_date,
                TARGET.time_zone = SOURCE.time_zone,
                TARGET.currency = SOURCE.currency,
                TARGET.amount = SOURCE.amount,
                TARGET.type = SOURCE.type, 
                TARGET.risk_scoring = SOURCE.risk_scoring,
                TARGET.shopper_interaction = SOURCE.shopper_interaction, 
                TARGET.shopper_country = SOURCE.shopper_country, 
                TARGET.issuer_name = SOURCE.issuer_name,
                TARGET.issuer_id = SOURCE.issuer_id,
                TARGET.issuer_country = SOURCE.issuer_country,
                TARGET.shopper_email = SOURCE.shopper_email,  
                TARGET.shopper_reference = SOURCE.shopper_reference,
                TARGET.[3d_directory_response] = SOURCE.[3d_directory_response],
                TARGET.[3d_authentication_response] = SOURCE.[3d_authentication_response],
                TARGET.cvc2_response = SOURCE.cvc2_response,
                TARGET.avs_response = SOURCE.avs_response,
                TARGET.acquirer_response = SOURCE.acquirer_response,
                TARGET.raw_acquirer_response = SOURCE.raw_acquirer_response,
                TARGET.authorisation_code = SOURCE.authorisation_code,
                TARGET.acquirer_reference = SOURCE.acquirer_reference,
                TARGET.payment_method_variant = SOURCE.payment_method_variant                            
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                company_account,
                merchant_account, 
                psp_reference, 
                merchant_reference,
                payment_method,
                creation_date,
                time_zone,
                currency,
                amount,
                type, 
                risk_scoring,
                shopper_interaction,
                shopper_country, 
                issuer_name,
                issuer_id,
                issuer_country,
                shopper_email,
                shopper_reference,
                [3d_directory_response],
                [3d_authentication_response],
                cvc2_response,
                avs_response,
                acquirer_response,
                raw_acquirer_response,
                authorisation_code,
                acquirer_reference,
                payment_method_variant) 
        VALUES (
                SOURCE.company_account,
                SOURCE.merchant_account, 
                SOURCE.psp_reference, 
                SOURCE.merchant_reference,
                SOURCE.payment_method,
                SOURCE.creation_date,
                SOURCE.time_zone,
                SOURCE.currency,
                SOURCE.amount,
                SOURCE.type, 
                SOURCE.risk_scoring,
                SOURCE.shopper_interaction,
                SOURCE.shopper_country, 
                SOURCE.issuer_name,
                SOURCE.issuer_id,
                SOURCE.issuer_country,
                SOURCE.shopper_email,
                SOURCE.shopper_reference,
                SOURCE.[3d_directory_response],
                SOURCE.[3d_authentication_response],
                SOURCE.cvc2_response,
                SOURCE.avs_response,
                SOURCE.acquirer_response,
                SOURCE.raw_acquirer_response,
                SOURCE.authorisation_code,
                SOURCE.acquirer_reference,
                SOURCE.payment_method_variant);
        """

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[payments_received_temp]"

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
