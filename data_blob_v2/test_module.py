from unittest.mock import MagicMock, Mock, patch
import dependency_injector
import pytest

from services import AuthService, BlobContainerService
from main import main
from containers import Container


@pytest.fixture
def container():
    container = Container(
        config={
            'dcinternal': {
                'scope': 'data-prd-keyvault',
                'key_name': 'dcinternal-account-key'}})
    #container.config.from_yaml('./data_blob_v2/config.yml', required=True)
    return container


def test_module(container):
    #container = Container()

    # https://python-dependency-injector.ets-labs.org/providers/configuration.html#loading-from-a-dictionary
    #container.config.from_dict({'dcinternal': {'scope': "data-prd-keyvault", 'key_name':'dcinternal-account-key'}})

    dbutils = Mock()
    dbutils.secrets.get = MagicMock(return_value='key')

    container.init_resources()
    container.wire(modules=[__name__])

    result = container.blob_container_service().select(
        dbutils=dbutils, container_name='', options='', file_path='')

    assert result
    assert container.config.dcinternal.scope() == 'data-prd-keyvault'
    assert container.config.dcinternal.key_name() == 'dcinternal-account-key'
