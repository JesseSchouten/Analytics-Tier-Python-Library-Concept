class CustomersDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """MERGE customers AS TARGET USING customers_temp AS SOURCE
        ON SOURCE.customer_id = TARGET.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                TARGET.channel = SOURCE.channel,
                TARGET.country = SOURCE.country,
                TARGET.first_name = SOURCE.first_name,
                TARGET.last_name = SOURCE.last_name,
                TARGET.registration_country = SOURCE.registration_country,
                TARGET.create_date = SOURCE.create_date,
                TARGET.activation_date = SOURCE.activation_date,
                TARGET.last_login = SOURCE.last_login,
                TARGET.facebook_id = SOURCE.facebook_id,
                TARGET.email = SOURCE.email,
                TARGET.first_payment_date = SOURCE.first_payment_date
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                customer_id,
                channel,
                country,
                first_name,
                last_name,
                registration_country,
                create_date,
                activation_date,
                last_login,
                facebook_id,
                email,
                first_payment_date
            ) VALUES (
                SOURCE.customer_id, 
                SOURCE.channel,
                SOURCE.country,
                SOURCE.first_name,
                SOURCE.last_name,
                SOURCE.registration_country,
                SOURCE.create_date,
                SOURCE.activation_date,
                SOURCE.last_login,
                SOURCE.facebook_id,
                SOURCE.email,
                SOURCE.first_payment_date
            );"""

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[customers_temp]"

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