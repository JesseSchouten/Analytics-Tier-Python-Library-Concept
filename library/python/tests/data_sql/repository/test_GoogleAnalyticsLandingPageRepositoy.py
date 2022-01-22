import data_sql.main as lib
from unittest.mock import patch
from unittest.mock import MagicMock, Mock


@patch('data_sql.dao.prd.GoogleAnalyticsLandingPageDao')
def test_insert_success(GoogleAnalyticsLandingPageDao):
    context = lib.main(environment='test',
                       mock=GoogleAnalyticsLandingPageDao)
    GoogleAnalyticsLandingPageDao.insert = MagicMock(return_value=True)
    ga_repository = context['GoogleAnalyticsLandingPageRepository']
    data = []

    # act
    result = ga_repository.insert(data)

    # assert
    GoogleAnalyticsLandingPageDao.insert.assert_called_once()
    assert result is True


@patch('data_sql.dao.prd.GoogleAnalyticsLandingPageDao')
def test_insert_fails(GoogleAnalyticsLandingPageDao):
    context = lib.main(environment='test',
                       mock=GoogleAnalyticsLandingPageDao)
    GoogleAnalyticsLandingPageDao.insert = MagicMock(return_value=False)
    ga_repository = context['GoogleAnalyticsLandingPageRepository']
    data = []

    # act
    result = ga_repository.insert(data)

    # assert
    GoogleAnalyticsLandingPageDao.insert.assert_called_once()
    assert result is False


@patch('data_sql.dao.prd.GoogleAnalyticsLandingPageDao')
def test_insert_error(GoogleAnalyticsLandingPageDao):
    # arange
    context = lib.main(environment='test',
                       mock=GoogleAnalyticsLandingPageDao)
    GoogleAnalyticsLandingPageDao.insert = Mock(
        side_effect=Exception('no connection available'))
    ga_repository = context['GoogleAnalyticsLandingPageRepository']
    data = []

    # act
    result = ga_repository.insert(data)

    # assert
    GoogleAnalyticsLandingPageDao.insert.assert_called()
    assert result is False
