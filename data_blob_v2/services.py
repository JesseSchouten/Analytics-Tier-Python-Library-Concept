import logging
from typing import Dict


class BaseService:
    def __init__(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}",
        )


class AuthService(BaseService):
    def __init__(self, scope: str, key_name: str) -> None:
        super().__init__()
        self.scope = scope
        self.key_name = key_name

    def get_account_key(self, dbutils):
        print(self.scope)
        print(self.key_name)
        return dbutils.secrets.get(scope=self.scope, key=self.key_name)


class BlobContainerService(BaseService):
    def __init__(self, auth_service: AuthService) -> None:
        super().__init__()
        self.auth_service = auth_service

    def select(self, dbutils, container_name: str,
               options: dict, file_path: str) -> Dict[str, str]:
        account_key = self.auth_service.get_account_key(dbutils)
        file = "wasbs://{}@{}.blob.core.windows.net/{}".format(
            container_name, '', file_path)
        try:
            print(f"read file {file} using options {options}")
            return True
        except Exception as e:
            return False
