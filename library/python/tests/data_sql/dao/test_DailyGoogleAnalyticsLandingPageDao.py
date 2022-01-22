import data_sql.main as lib


def test_get_upsert():
    ctx = lib.main(environment='test')
    GoogleAnalyticsLandingPageDao = ctx['GoogleAnalyticsLandingPageDao']
    query = GoogleAnalyticsLandingPageDao.get_upsert_query()
    assert query is not None
