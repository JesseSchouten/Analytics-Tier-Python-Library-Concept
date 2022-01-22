class GoogleAnalyticsDailyGoalsDao:
    def __init__(self, context):
        self.context = context

    def get_upsert_query(self):
        return """
        MERGE daily_ga_goals_report AS TARGET 
        USING daily_ga_goals_report_temp AS SOURCE
        ON SOURCE.date = TARGET.date
            AND SOURCE.account_id = TARGET.account_id
            AND SOURCE.property_id = TARGET.property_id
            AND SOURCE.view_id = TARGET.view_id
            AND SOURCE.channel = TARGET.channel
            AND SOURCE.country = TARGET.country
            AND SOURCE.segment = TARGET.segment
            AND SOURCE.source = TARGET.source
            AND SOURCE.medium = TARGET.medium
            AND SOURCE.campaign_name = TARGET.campaign_name
            AND SOURCE.ad_name = TARGET.ad_name
        WHEN MATCHED THEN
            UPDATE SET
                TARGET.date = SOURCE.date,
                TARGET.account_id = SOURCE.account_id,
                TARGET.property_id = SOURCE.property_id,
                TARGET.property_name = SOURCE.property_name,
                TARGET.view_id = SOURCE.view_id,
                TARGET.view_name = SOURCE.view_name,
                TARGET.view_timezone = SOURCE.view_timezone,
                TARGET.channel = SOURCE.channel,
                TARGET.country = SOURCE.country,
                TARGET.segment = SOURCE.segment,
                TARGET.source = SOURCE.source,
                TARGET.medium = SOURCE.medium,
                TARGET.campaign_name = SOURCE.campaign_name,
                TARGET.ad_name = SOURCE.ad_name,
                TARGET.sessions = SOURCE.sessions,
                TARGET.new_users = SOURCE.new_users,
                TARGET.pageviews = SOURCE.pageviews,
                TARGET.bounces = SOURCE.bounces,
                TARGET.goal_1_completions = SOURCE.goal_1_completions,
                TARGET.goal_2_completions = SOURCE.goal_2_completions,
                TARGET.goal_3_completions = SOURCE.goal_3_completions,
                TARGET.goal_4_completions = SOURCE.goal_4_completions,
                TARGET.goal_5_completions = SOURCE.goal_5_completions,
                TARGET.goal_6_completions = SOURCE.goal_6_completions,
                TARGET.goal_7_completions = SOURCE.goal_7_completions,
                TARGET.goal_8_completions = SOURCE.goal_8_completions,
                TARGET.goal_9_completions = SOURCE.goal_9_completions,
                TARGET.goal_10_completions = SOURCE.goal_10_completions,
                TARGET.goal_11_completions = SOURCE.goal_11_completions,
                TARGET.goal_12_completions = SOURCE.goal_12_completions,
                TARGET.goal_13_completions = SOURCE.goal_13_completions,
                TARGET.goal_14_completions = SOURCE.goal_14_completions,
                TARGET.goal_15_completions = SOURCE.goal_15_completions,
                TARGET.goal_16_completions = SOURCE.goal_16_completions,
                TARGET.goal_17_completions = SOURCE.goal_17_completions,
                TARGET.goal_18_completions = SOURCE.goal_18_completions,
                TARGET.goal_19_completions = SOURCE.goal_19_completions,
                TARGET.goal_20_completions = SOURCE.goal_20_completions
        WHEN NOT MATCHED BY TARGET THEN 
            INSERT (
                date,
                account_id,
                property_id,
                property_name,
                view_id,
                view_name,
                view_timezone,
                channel,
                country,
                segment,
                source,
                medium,
                campaign_name,
                ad_name,
                sessions,
                new_users,
                pageviews,
                bounces,
                goal_1_completions,
                goal_2_completions,
                goal_3_completions,
                goal_4_completions,
                goal_5_completions,
                goal_6_completions,
                goal_7_completions,
                goal_8_completions,
                goal_9_completions,
                goal_10_completions,
                goal_11_completions,
                goal_12_completions,
                goal_13_completions,
                goal_14_completions,
                goal_15_completions,
                goal_16_completions,
                goal_17_completions,
                goal_18_completions,
                goal_19_completions,
                goal_20_completions 
            ) 
            VALUES (
                SOURCE.date,
                SOURCE.account_id,
                SOURCE.property_id,
                SOURCE.property_name,
                SOURCE.view_id,
                SOURCE.view_name,
                SOURCE.view_timezone,
                SOURCE.channel,
                SOURCE.country,
                SOURCE.segment,
                SOURCE.source,
                SOURCE.medium,
                SOURCE.campaign_name,
                SOURCE.ad_name,
                SOURCE.sessions,
                SOURCE.new_users,
                SOURCE.pageviews,
                SOURCE.bounces,
                SOURCE.goal_1_completions,
                SOURCE.goal_2_completions,
                SOURCE.goal_3_completions,
                SOURCE.goal_4_completions,
                SOURCE.goal_5_completions,
                SOURCE.goal_6_completions,
                SOURCE.goal_7_completions,
                SOURCE.goal_8_completions,
                SOURCE.goal_9_completions,
                SOURCE.goal_10_completions,
                SOURCE.goal_11_completions,
                SOURCE.goal_12_completions,
                SOURCE.goal_13_completions,
                SOURCE.goal_14_completions,
                SOURCE.goal_15_completions,
                SOURCE.goal_16_completions,
                SOURCE.goal_17_completions,
                SOURCE.goal_18_completions,
                SOURCE.goal_19_completions,
                SOURCE.goal_20_completions
            );
            """

    def insert(self, data):
        sql_connector = self.context['SQLConnector']

        table = "[dbo].[daily_ga_goals_report_temp]"

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
            # Remove temp table
            delete_query = f"DROP TABLE {table}"
            cursor.execute(delete_query)

            sql_connector.conn.commit()
            sql_connector.close_connection()
        return True
