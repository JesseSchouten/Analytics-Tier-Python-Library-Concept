from BlobContainer import BlobContainer
from main import main

from unittest.mock import MagicMock, Mock, patch

def test_adx_table():
    dbutils = Mock()
    dbutils.secrets.get = MagicMock(return_value = 'key')
    ctx = main(environment = 'test', dbutils=dbutils)

    blob = BlobContainer(context=ctx)
    assert blob.select_from_csv('','', options={}) == True