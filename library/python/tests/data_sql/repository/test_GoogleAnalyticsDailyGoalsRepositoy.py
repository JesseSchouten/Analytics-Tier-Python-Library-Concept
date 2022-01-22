import data_sql.main as lib
from unittest.mock import patch
from unittest.mock import MagicMock, Mock


@patch('data_sql.dao.prd.GoogleAnalyticsDailyGoalsDao')
def test_insert_success(MockGoogleAnalyticsDailyGoalsDao):
    context = lib.main(environment='test',
                       mock=MockGoogleAnalyticsDailyGoalsDao)
    MockGoogleAnalyticsDailyGoalsDao.insert = MagicMock(return_value=True)
    ga_repository = context['GoogleAnalyticsDailyGoalsRepository']
    data = []

    # act
    result = ga_repository.insert(data)

    # assert
    MockGoogleAnalyticsDailyGoalsDao.insert.assert_called_once()
    assert result is True


@patch('data_sql.dao.prd.GoogleAnalyticsDailyGoalsDao')
def test_insert_failure(MockGoogleAnalyticsDailyGoalsDao):
    # arange
    context = lib.main(environment='test',
                       mock=MockGoogleAnalyticsDailyGoalsDao)
    MockGoogleAnalyticsDailyGoalsDao.insert = MagicMock(return_value=False)
    ga_repository = context['GoogleAnalyticsDailyGoalsRepository']
    data = []

    # act
    result = ga_repository.insert(data)

    # assert
    MockGoogleAnalyticsDailyGoalsDao.insert.assert_called_once()
    assert result is False


@patch('data_sql.dao.prd.GoogleAnalyticsDailyGoalsDao')
def test_insert_error(MockGoogleAnalyticsDailyGoalsDao):
    # arange
    context = lib.main(environment='test',
                       mock=MockGoogleAnalyticsDailyGoalsDao)
    MockGoogleAnalyticsDailyGoalsDao.insert = Mock(
        side_effect=Exception('no connection available'))
    ga_repository = context['GoogleAnalyticsDailyGoalsRepository']
    data = []

    # act
    result = ga_repository.insert(data)

    # assert
    MockGoogleAnalyticsDailyGoalsDao.insert.assert_called()
    assert result is False
