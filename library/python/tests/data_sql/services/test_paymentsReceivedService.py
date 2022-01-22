import data_sql.main as lib
from pyspark.sql import DataFrame

from unittest.mock import MagicMock, Mock, patch

@patch('data_sql.services.PaymentsReceivedService')
def test_columns_failed(MockPaymentsReceivedService):
    context = lib.main(environment='test',
                mock=MockPaymentsReceivedService)

    payments_received_service = context['PaymentsReceivedService']

    MockPaymentsReceivedService.insert = MagicMock(return_value=True)

    data = Mock(spec=DataFrame) 
     
    data.columns = payments_received_service.columns.copy()
    data.columns = data.columns.pop(0)

    result = payments_received_service.run_insert(data)

    assert result == False

@patch('data_sql.services.PaymentsReceivedService')
def test_datatype_failed(MockPaymentsReceivedService):
    context = lib.main(environment='test',
                mock=MockPaymentsReceivedService)

    payments_received_service = context['PaymentsReceivedService']

    MockPaymentsReceivedService.insert = MagicMock(return_value=True)

    data = Mock(spec=[])
     
    data.columns = payments_received_service.columns.copy()

    result = payments_received_service.run_insert(data)

    assert result == False