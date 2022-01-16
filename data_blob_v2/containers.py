from dependency_injector import containers, providers

import services


class Container(containers.DeclarativeContainer):
    # Services
    config = providers.Configuration(yaml_files=["config.yml"])
    #config = providers.Configuration()

    auth_service = providers.Factory(services.AuthService,
                                    scope = config.dcinternal.scope,
                                    key_name = config.dcinternal.key_name
                                    )

    blob_container_service = providers.Factory(
        services.BlobContainerService,
        auth_service= providers.Factory(auth_service)      
    )



