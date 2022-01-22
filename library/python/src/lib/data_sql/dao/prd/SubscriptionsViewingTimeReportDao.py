class SubscriptionsViewingTimeReportDao:
    def __init__(self, context):
        self.context = context

    def get_insert_query(self):
        return '''INSERT INTO subscriptions_viewing_time_report(
        id, channel, customer_id, customer_registration_country
        , subscription_created_date,subscription_start_date
        , subscription_plan_id,subscription_plan_billingcycle_type
        ,subscription_plan_billingfrequency,subscription_plan_price,subscription_plan_currency) 
        VALUES(?,?,?,?,?,?,?,?,?,?,?)'''

    def get_update_query(self):
        return '''UPDATE subscriptions_viewing_time_report SET channel=?, customer_id=?
        , customer_registration_country=?, subscription_created_date=?
        ,subscription_start_date=?, subscription_plan_id=?
        ,subscription_plan_billingcycle_type=?,subscription_plan_billingfrequency=?
        ,subscription_plan_price=?,subscription_plan_currency=?, updated = CURRENT_TIMESTAMP 
        WHERE id=?'''

    def get_upsert_query(self):
        return '''EXEC Upsert_SubscriptionsViewingTimeReport @id=?, @channel=?, @customer_id=?
        , @customer_registration_country=?, @subscription_created_date=?,@subscription_start_date=?
        , @subscription_plan_id=?,@subscription_plan_billingcycle_type=?
        ,@subscription_plan_billingfrequency=?,@subscription_plan_price=?,@subscription_plan_currency=?'''

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        sql_connector.open_connection(conn_type='odbc')

        cursor = sql_connector.conn.cursor()
        counter = 1

        for row in data:
            if counter % batch_size == 0:
                cursor.commit()

            insert_row = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]]
            cursor.execute(self.get_upsert_query(), insert_row)

            counter += 1

        cursor.commit()
        sql_connector.close_connection()

        return True
