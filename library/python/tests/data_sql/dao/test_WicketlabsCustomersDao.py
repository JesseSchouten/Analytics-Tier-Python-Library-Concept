import data_sql.main as lib

def test_get_upsert():
    ctx = lib.main(environment='test')
    wicketlabs_customers_doa = ctx['WicketlabsCustomersDao']
    query = wicketlabs_customers_doa.get_upsert_query()
    print(query)
    assert query is not None

