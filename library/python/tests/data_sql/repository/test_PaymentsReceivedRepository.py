import data_sql.main as lib

from unittest.mock import MagicMock, Mock, patch

@patch('data_sql.dao.prd.PaymentsReceivedDao')
def test_insert_succes(MockPaymentsReceivedDao):
    context = lib.main(environment='test',
                    mock=MockPaymentsReceivedDao)
    
    payments_received_repository = context['PaymentsReceivedRepository']

    MockPaymentsReceivedDao.insert = MagicMock(return_value=True)

    data = []

    result = payments_received_repository.insert(data)

    assert result == True


