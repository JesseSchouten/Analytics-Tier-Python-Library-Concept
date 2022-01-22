class ViewingDao:
    def __init__(self, context):
        self.context = context

    def insert(self, data, batch_size=10000):
        sql_connector = self.context['SQLConnector']

        sql_connector.open_connection(conn_type='odbc_sqlalchemy')

        data.to_sql(name='viewing', con=sql_connector.conn, chunksize=batch_size,
                    schema=None, if_exists='append', index=False)

        return True
