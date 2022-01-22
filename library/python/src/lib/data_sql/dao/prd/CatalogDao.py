class CatalogDao:
    def __init__(self, context):
        self.context = context

    def get_insert_query(self):
        return '''INSERT INTO catalog (id, asset_type, asset_subtype
        , channel, title, genre, tags) VALUES(?,?,?,?,?,?,?)'''

    def get_update_query(self):
        return '''UPDATE catalog SET asset_type=?, asset_subtype=?
        , title=?, genre=?, tags=?, updated = CURRENT_TIMESTAMP WHERE id=? AND channel=?'''

    def get_upsert_query(self):
        return """EXEC Upsert_Catalog @id=?, @asset_type=?
        , @asset_subtype=?, @channel=?, @title=?,@genre=?, @tags=?"""

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        sql_connector.open_connection(conn_type='odbc')

        cursor = sql_connector.conn.cursor()
        counter = 1

        for row in data:
            if counter % batch_size == 0:
                cursor.commit()

            insert_row = [row[0], row[1], row[2], row[3], row[4], row[5], row[6]]
            cursor.execute(self.get_upsert_query(), insert_row)

            counter += 1

        cursor.commit()
        sql_connector.close_connection()

        return True
