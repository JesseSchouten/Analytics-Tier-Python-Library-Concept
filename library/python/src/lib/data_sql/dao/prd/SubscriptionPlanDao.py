class SubscriptionPlanDao:
    def __init__(self, context):
        self.context = context

    def get_insert_query(self):
        return 'INSERT INTO subscription_plan(id, channel, country, ' \
               'pause_max_days, system, free_trial, billing_cycle_type, ' \
               'description, price, asset_type, only_available_with_promotion, ' \
               'billing_frequency, recurring, catch_up_hours, pause_min_days, ' \
               'number_of_supported_devices, currency, business_type, is_downloadable, ' \
               'only_available_for_logged_in_users, title, start, end, ' \
               'subscription_plan_type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

    def get_update_query(self):
        return 'UPDATE subscription_plan SET ' \
               'channel=?, country=?, ' \
               'pause_max_days=?, system=?, free_trial=?, billing_cycle_type=?, ' \
               'description=?, price=?, asset_type=?, only_available_with_promotion=?, ' \
               'billing_frequency=?, recurring=?, catch_up_hours=?, pause_min_days=?, ' \
               'number_of_supported_devices=?, currency=?, business_type=?, is_downloadable=?, ' \
               'only_available_for_logged_in_users=?, title=?, start=?, end=?, ' \
               'subscription_plan_type=?' \
               'updated = CURRENT_TIMESTAMP ' \
               'WHERE id=?'

    def get_upsert_query(self):
        return 'EXEC Upsert_SubscriptionPlan ' \
               '@id=?, @channel=?, @country=?, ' \
               '@pause_max_days=?, @system=?, @free_trial=?, @billing_cycle_type=?, ' \
               '@description=?, @price=?, @asset_type=?, @only_available_with_promotion=?, ' \
               '@billing_frequency=?, @recurring=?, @catch_up_hours=?, @pause_min_days=?, ' \
               '@number_of_supported_devices=?, @currency=?, @business_type=?, @is_downloadable=?, ' \
               '@only_available_for_logged_in_users=?, @title=?, @start=?, @end=?, ' \
               '@subscription_plan_type=?'

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        sql_connector.open_connection(conn_type='odbc')

        cursor = sql_connector.conn.cursor()
        counter = 1

        for row in data:
            if counter % batch_size == 0:
                cursor.commit()


            insert_row = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                          row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],
                          row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23]]

            cursor.execute(self.get_upsert_query(), insert_row)

            counter += 1

        cursor.commit()
        sql_connector.close_connection()

        return True
