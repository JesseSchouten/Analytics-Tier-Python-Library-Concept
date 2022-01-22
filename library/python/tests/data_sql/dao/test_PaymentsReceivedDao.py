import data_sql.main as lib

def test_get_upsert():
    ctx = lib.main(environment='dev')
    payments_received_dao = ctx['PaymentsReceivedDao']
    query = payments_received_dao.get_upsert_query()
    print(query)
    assert query is not None