import io


class WicketlabsCustomersDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """MERGE wicketlabs_customers AS TARGET USING wicketlabs_customers_temp AS SOURCE
        ON SOURCE.customer_id = TARGET.customer_id
        WHEN MATCHED AND SOURCE.date > TARGET.date THEN
            UPDATE SET
                TARGET.customer_id = SOURCE.customer_id,  
                TARGET.subscription_Id = SOURCE.subscription_Id,  
                TARGET.active_start_date = SOURCE.active_start_date,
                TARGET.trial_start_date = SOURCE.trial_start_date,  
                TARGET.active_end_date = SOURCE.active_end_date,  
                TARGET.channel = SOURCE.channel,  
                TARGET.product_name = SOURCE.product_name,
                TARGET.is_reconnect = SOURCE.is_reconnect,
                TARGET.used_special_offer = SOURCE.used_special_offer,  
                TARGET.subscription_unit = SOURCE.subscription_unit,
                TARGET.unit_price = SOURCE.unit_price,  
                TARGET.unit_price_currency = SOURCE.unit_price_currency,  
                TARGET.total_payments = SOURCE.total_payments,
                TARGET.last_view_date = SOURCE.last_view_date,
                TARGET.days_since_last_engagement = SOURCE.days_since_last_engagement,
                TARGET.tenure = SOURCE.tenure,
                TARGET.engagement_status = SOURCE.engagement_status,  
                TARGET.primary_device_life_time = SOURCE.primary_device_life_time,
                TARGET.primary_device_last_three_months = SOURCE.primary_device_last_three_months,
                TARGET.primary_viewing_location_life_time = SOURCE.primary_viewing_location_life_time,
                TARGET.primary_viewing_location_last_three_months = SOURCE.primary_viewing_location_last_three_months,  
                TARGET.hours_viewed_last_three_months = SOURCE.hours_viewed_last_three_months,
                TARGET.customer_subscription_count = SOURCE.customer_subscription_count,
                TARGET.customer_tenure = SOURCE.customer_tenure,
                TARGET.customer_voluntary_cancellation_count = SOURCE.customer_voluntary_cancellation_count,
                TARGET.customer_hours_viewed = SOURCE.customer_hours_viewed,
                TARGET.entertainment_index_last_three_months = SOURCE.entertainment_index_last_three_months,
                TARGET.entertainment_index_lifetime = SOURCE.entertainment_index_lifetime,
                TARGET.date = SOURCE.date,  
                TARGET.id = SOURCE.id
        WHEN NOT MATCHED BY TARGET THEN 
            INSERT (customer_id, subscription_Id, active_start_date,
                        trial_start_date, active_end_date, channel, product_name,
                        is_reconnect, used_special_offer, subscription_unit,
                        unit_price, unit_price_currency, total_payments,
                        last_view_date, days_since_last_engagement, tenure,
                        engagement_status, primary_device_life_time, primary_device_last_three_months,
                        primary_viewing_location_life_time, primary_viewing_location_last_three_months, 
                        hours_viewed_last_three_months, customer_subscription_count, customer_tenure, customer_voluntary_cancellation_count,
                        customer_hours_viewed, entertainment_index_last_three_months, entertainment_index_lifetime,
                        date, id) VALUES (
                SOURCE.customer_id, 
                SOURCE.subscription_Id, 
                SOURCE.active_start_date,
                SOURCE.trial_start_date, 
                SOURCE.active_end_date, 
                SOURCE.channel, 
                SOURCE.product_name,
                SOURCE.is_reconnect,
                SOURCE.used_special_offer, 
                SOURCE.subscription_unit,
                SOURCE.unit_price, 
                SOURCE.unit_price_currency, 
                SOURCE.total_payments,
                SOURCE.last_view_date,
                SOURCE.days_since_last_engagement,
                SOURCE.tenure,
                SOURCE.engagement_status, 
                SOURCE.primary_device_life_time,
                SOURCE.primary_device_last_three_months,
                SOURCE.primary_viewing_location_life_time,
                SOURCE.primary_viewing_location_last_three_months, 
                SOURCE.hours_viewed_last_three_months,
                SOURCE.customer_subscription_count,
                SOURCE.customer_tenure,
                SOURCE.customer_voluntary_cancellation_count,
                SOURCE.customer_hours_viewed,
                SOURCE.entertainment_index_last_three_months,
                SOURCE.entertainment_index_lifetime,
                SOURCE.date, 
                SOURCE.id
            );"""

    def insert(self, data):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[wicketlabs_customers_temp]"

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
