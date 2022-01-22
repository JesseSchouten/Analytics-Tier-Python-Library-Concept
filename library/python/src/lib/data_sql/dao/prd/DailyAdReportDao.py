class DailyAdReportDao:
    def __init__(self, context):
        self.context = context

    def get_insert_query(self):
        return None

    def get_update_query(self):
        return None

    def get_upsert_query(self):
        return """
        MERGE daily_ad_report AS TARGET  
        USING daily_ad_report_temp AS SOURCE 
        ON SOURCE.date = TARGET.date
            AND SOURCE.channel = TARGET.channel
            AND SOURCE.country = TARGET.country
            AND SOURCE.social_channel = TARGET.social_channel
            AND SOURCE.account_id = TARGET.account_id
            AND SOURCE.campaign_id = TARGET.campaign_id
            AND SOURCE.adset_id = TARGET.adset_id
            AND SOURCE.ad_id = TARGET.ad_id
        WHEN MATCHED THEN
            UPDATE SET 
                TARGET.time_zone = SOURCE.time_zone,
                TARGET.account_name = SOURCE.account_name,
                TARGET.campaign_name = SOURCE.campaign_name,
                TARGET.adset_name = SOURCE.adset_name,
                TARGET.ad_name = SOURCE.ad_name,
                TARGET.currency = SOURCE.currency,
                TARGET.paid_impressions = SOURCE.paid_impressions,
                TARGET.paid_clicks = SOURCE.paid_clicks,
                TARGET.costs = SOURCE.costs         
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                date,
                channel,
                country,
                social_channel, 
                account_id,
                account_name, 
                campaign_id, 
                campaign_name, 
                adset_id,
                adset_name, 
                ad_id, 
                ad_name,
                time_zone, 
                currency, 
                paid_impressions,
                paid_clicks, 
                costs
            ) 
        VALUES (
                SOURCE.date,
                SOURCE.channel,
                SOURCE.country,
                SOURCE.social_channel, 
                SOURCE.account_id,
                SOURCE.account_name, 
                SOURCE.campaign_id, 
                SOURCE.campaign_name, 
                SOURCE.adset_id,
                SOURCE.adset_name, 
                SOURCE.ad_id, 
                SOURCE.ad_name, 
                SOURCE.time_zone,
                SOURCE.currency, 
                SOURCE.paid_impressions,
                SOURCE.paid_clicks, 
                SOURCE.costs          
        );
        """

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[daily_ad_report_temp]"

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
