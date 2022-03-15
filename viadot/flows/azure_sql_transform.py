from typing import Any, Dict, List

from prefect import Flow

from ..tasks.azure_sql import AzureSQLDBQuery
from ..task_utils import upload_query_to_devops

query_task = AzureSQLDBQuery()
query_to_devops = upload_query_to_devops


class AzureSQLTransform(Flow):
    def __init__(
        self,
        name: str,
        query: str,
        save_query: bool = False,
        file_path: str = None,
        project: str = None,
        wiki_identifier: str = None,
        devops_path: str = None,
        personal_access_token: str = None,
        organization_url: str = None,
        sqldb_credentials_secret: str = None,
        vault_name: str = None,
        tags: List[str] = ["transform"],
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """
        Flow for running SQL queries on top of Azure SQL Database.

        Args:
            name (str): The name of the flow.
            query (str, required): The query to execute on the database.
            file_path (str, optional): Path where to save a query. Defaults to None.
            project (str, optional): Name od DevOps project. Defaults to None.
            wiki_identifier (str, optional): Name of DevOps Wiki. Defaults to None.
            devops_path (str, optional): Path to DevOps page where to upload query. Defaults to None.
            personal_access_token (str, optional): Presonl access token to Azure DevOps. Defaults to None.
                Instruction how to create PAT: https://docs.microsoft.com/en-us/azure/devops/organizations/accounts/use-personal-access-tokens-to-authenticate
            organization_url(str, optional): Service URL. Defaults to None.
            save_query (bool, optional): Whether to save a query. Defaults to False.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with SQL db credentials (server, db_name, user, and password).
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            tags (list, optional): Tag for marking flow. Defaults to "transform".
        """
        self.query = query
        self.tags = tags
        self.save_query = save_query
        self.file_path = file_path
        self.sqldb_credentials_secret = sqldb_credentials_secret
        self.vault_name = vault_name
        self.tasks = [query_task]

        # AzureDevOps API params
        self.project = project
        self.wiki_identifier = wiki_identifier
        self.devops_path = devops_path
        self.personal_access_token = personal_access_token
        self.organization_url = organization_url

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:
        query_task.bind(
            query=self.query,
            credentials_secret=self.sqldb_credentials_secret,
            vault_name=self.vault_name,
            save_query=self.save_query,
            file_path=self.file_path,
            flow=self,
        )
        if self.save_query is True:
            query_to_devops.bind(
                project=self.project,
                wiki_identifier=self.wiki_identifier,
                devops_path=self.devops_path,
                organization_url=self.organization_url,
                file_path=self.file_path,
                personal_access_token=self.personal_access_token,
                flow=self,
            )
            query_to_devops.set_upstream(query_task, flow=self)
