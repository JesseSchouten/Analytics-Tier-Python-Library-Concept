from dependency_injector import containers, providers

from .services import AuthService, BlobContainerService


class Container(containers.DeclarativeContainer):
    # Services
    config = providers.Configuration(yaml_files=["config.yml"])
    #config = providers.Configuration()

    auth_service = providers.Factory(AuthService,
                                    storage_account_name = 'dcinternal',
                                    scope = config.dcinternal.scope,
                                    key_name = config.dcinternal.key_name
                                    )

    blob_container_service = providers.Factory(
        BlobContainerService,
        auth_service = providers.Factory(auth_service)      
    )



