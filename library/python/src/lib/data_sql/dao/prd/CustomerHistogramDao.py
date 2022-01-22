class CustomerHistogramDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """MERGE customer_histogram AS TARGET USING customer_histogram_temp AS SOURCE
        ON SOURCE.channel = TARGET.channel AND SOURCE.cohort = TARGET.cohort AND SOURCE.age = TARGET.age
        WHEN MATCHED THEN
            UPDATE SET
                TARGET.recurring = SOURCE.recurring,
                TARGET.cancelled = SOURCE.cancelled,
                TARGET.total = SOURCE.total,
                TARGET.older_than = SOURCE.older_than,
                TARGET.churn = SOURCE.churn
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (channel, cohort, age, recurring, cancelled, total, older_than, churn) VALUES (
                SOURCE.channel,
                SOURCE.cohort,
                SOURCE.age,
                SOURCE.recurring,
                SOURCE.cancelled,
                SOURCE.total,
                SOURCE.older_than,
                SOURCE.churn
            );"""

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[customer_histogram_temp]"

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
