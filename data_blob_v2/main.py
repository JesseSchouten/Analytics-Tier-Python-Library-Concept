
from dependency_injector.wiring import Provide, inject

from containers import Container
from services import  BlobContainerService


@inject
def main(blob_container_service: BlobContainerService = Provide[Container.blob_container_service]) -> None:

    ctx = {}
    ctx['BlobContainerService'] = blob_container_service

    return ctx