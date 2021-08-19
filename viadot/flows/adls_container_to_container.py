from typing import Any, Dict, List

from prefect import Flow, task
from prefect.storage import Local
from prefect.utilities import logging

from ..tasks import AzureDataLakeCopy

copy_task = AzureDataLakeCopy()

logger = logging.get_logger(__name__)


@task
def is_stored_locally(f: Flow):
    return f.storage is None or isinstance(f.storage, Local)


class ADLSContainerToContainer(Flow):
    """Copy file(s) between containers.

    Args:
        name (str): The name of the flow.
        from_path (str): The path to the Data Lake file.
        to_path (str): The path of the final file location a/a/filename.extension.
        adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        vault_name (str): The name of the vault from which to retrieve the secrets.
    """

    def __init__(
        self,
        name: str,
        from_path: str,
        to_path: str,
        adls_sp_credentials_secret: str = None,
        vault_name: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):

        from_path = from_path.strip("/")
        self.from_path = from_path
        self.to_path = to_path
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.vault_name = vault_name
        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        copy_task.bind(
            from_path=self.from_path,
            to_path=self.to_path,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )
