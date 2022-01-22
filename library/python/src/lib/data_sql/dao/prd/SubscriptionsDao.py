class SubscriptionsDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """MERGE subscriptions AS TARGET USING subscriptions_temp AS SOURCE
        ON SOURCE.subscription_id = TARGET.subscription_id
        WHEN MATCHED THEN
            UPDATE SET
                TARGET.customer_id = SOURCE.customer_id,
                TARGET.channel = SOURCE.channel,
                TARGET.country = SOURCE.country,
                TARGET.subscription_plan_id = SOURCE.subscription_plan_id,
                TARGET.subscription_create_date = SOURCE.subscription_create_date,
                TARGET.subscription_start = SOURCE.subscription_start,
                TARGET.subscription_end = SOURCE.subscription_end,
                TARGET.subscription_paused_date = SOURCE.subscription_paused_date,
                TARGET.subscription_resumable_days = SOURCE.subscription_resumable_days,
                TARGET.state = SOURCE.state,
                TARGET.recurring_enabled = SOURCE.recurring_enabled,
                TARGET.payment_provider = SOURCE.payment_provider,
                TARGET.free_trial = SOURCE.free_trial,
                TARGET.notes = SOURCE.notes,
                TARGET.is_pausable = SOURCE.is_pausable,
                TARGET.is_resumable = SOURCE.is_resumable,
                TARGET.new_plan_id = SOURCE.new_plan_id,
                TARGET.first_payment_date = SOURCE.first_payment_date
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                subscription_id, 
                customer_id,
                channel,
                country,
                subscription_plan_id,
                subscription_create_date,
                subscription_start,
                subscription_end,
                subscription_paused_date,
                subscription_resumable_days,
                state,
                recurring_enabled,
                payment_provider,
                free_trial,
                notes,
                is_pausable,
                is_resumable,
                new_plan_id,
                first_payment_date
            ) VALUES (
                SOURCE.subscription_id, 
                SOURCE.customer_id,
                SOURCE.channel,
                SOURCE.country,
                SOURCE.subscription_plan_id,
                SOURCE.subscription_create_date,
                SOURCE.subscription_start,
                SOURCE.subscription_end,
                SOURCE.subscription_paused_date,
                SOURCE.subscription_resumable_days,
                SOURCE.state,
                SOURCE.recurring_enabled,
                SOURCE.payment_provider,
                SOURCE.free_trial,
                SOURCE.notes,
                SOURCE.is_pausable,
                SOURCE.is_resumable,
                SOURCE.new_plan_id,
                SOURCE.first_payment_date
            );"""

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[subscriptions_temp]"

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
