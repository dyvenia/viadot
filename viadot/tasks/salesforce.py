import json
from datetime import timedelta
from typing import Any, Dict, List

import pandas as pd
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Salesforce
from .azure_key_vault import AzureKeyVaultSecret


def get_credentials(credentials_secret: str, vault_name: str = None):
    """
    Get Salesforce credentials from Azure Key Vault.

    Args:
        credentials_secret (str): The name of the Azure Key Vault secret containing a dictionary with
            the required credentials (eg. username, password, token). Defaults to None.
        vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

    Returns: Credentials
    """
    if not credentials_secret:
        # attempt to read a default for the service principal secret name
        try:
            credentials_secret = PrefectSecret("SALESFORCE_DEFAULT_SECRET").run()
        except ValueError:
            pass

    if credentials_secret:
        azure_secret_task = AzureKeyVaultSecret()
        credentials_str = azure_secret_task.run(
            secret=credentials_secret, vault_name=vault_name
        )
        credentials = json.loads(credentials_str)

        return credentials


class SalesforceUpsert(Task):
    """
    Task for upserting a pandas DataFrame to Salesforce.

    Args:
    """

    def __init__(
        self,
        table: str = None,
        external_id: str = None,
        domain: str = "test",
        client_id: str = "viadot",
        env: str = "DEV",
        raise_on_error: bool = False,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.table = table
        self.external_id = external_id
        self.domain = domain
        self.client_id = client_id
        self.env = env
        self.raise_on_error = raise_on_error

        super().__init__(
            name="salesforce_upsert",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Upserting data to Salesforce"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "table",
        "external_id",
        "domain",
        "client_id",
        "env",
        "raise_on_error",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        df: pd.DataFrame = None,
        table: str = None,
        external_id: str = None,
        domain: str = None,
        client_id: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
        env: str = None,
        raise_on_error: bool = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> None:
        """Task run method.

        Args:
            df (pd.DataFrame, optional): The DataFrame to upsert. Defaults to None.
            table (str, optional): The table where the data should be upserted. Defaults to None.
            external_id (str, optional): The external ID to use for the upsert. Defaults to None.
            domain (str, optional): Domain of a connection; defaults to 'test' (sandbox).
                Can only be added if built-in username/password/security token is provided. Defaults to None.
            client_id (str, optional): Client id to keep the track of API calls. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                the required credentials (eg. username, password, token). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            env (str, optional): Environment information, provides information about credential and connection configuration. Defaults to 'DEV'.
            raise_on_error (bool, optional): Whether to raise an exception if a row upsert fails.
                If False, we only display a warning. Defaults to False.
        """
        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        salesforce = Salesforce(
            credentials=credentials,
            env=env,
            domain=domain,
            client_id=client_id,
        )
        self.logger.info(f"Upserting {df.shape[0]} rows to Salesforce...")
        salesforce.upsert(
            df=df, table=table, external_id=external_id, raise_on_error=raise_on_error
        )
        self.logger.info(f"Successfully upserted {df.shape[0]} rows to Salesforce.")


class SalesforceBulkUpsert(Task):
    """
    Task for upserting a pandas DataFrame to Salesforce.

    Args:
    """

    def __init__(
        self,
        table: str = None,
        external_id: str = None,
        domain: str = "test",
        client_id: str = "viadot",
        env: str = "DEV",
        raise_on_error: bool = False,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.table = table
        self.external_id = external_id
        self.domain = domain
        self.client_id = client_id
        self.env = env
        self.raise_on_error = raise_on_error

        super().__init__(
            name="salesforce_bulk_upsert",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Upserting data to Salesforce"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "table",
        "external_id",
        "domain",
        "client_id",
        "env",
        "raise_on_error",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        df: pd.DataFrame = None,
        table: str = None,
        external_id: str = None,
        batch_size: int = None,
        domain: str = None,
        client_id: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
        env: str = None,
        raise_on_error: bool = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> None:
        """Task run method.

        Args:
            df (pd.DataFrame, optional): The DataFrame to upsert. Defaults to None.
            table (str, optional): The table where the data should be upserted. Defaults to None.
            external_id (str, optional): The external ID to use for the upsert. Defaults to None.
            domain (str, optional): Domain of a connection; defaults to 'test' (sandbox).
                Can only be added if built-in username/password/security token is provided. Defaults to None.
            client_id (str, optional): Client id to keep the track of API calls. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                the required credentials (eg. username, password, token). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.
            env (str, optional): Environment information, provides information about credential and connection configuration. Defaults to 'DEV'.
            raise_on_error (bool, optional): Whether to raise an exception if a row upsert fails.
                If False, we only display a warning. Defaults to False.
        """
        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        salesforce = Salesforce(
            credentials=credentials,
            env=env,
            domain=domain,
            client_id=client_id,
        )
        self.logger.info(f"Upserting {df.shape[0]} rows to Salesforce...")
        salesforce.bulk_upsert(
            df=df,
            table=table,
            external_id=external_id,
            batch_size=batch_size,
            raise_on_error=raise_on_error,
        )
        self.logger.info(f"Successfully upserted {df.shape[0]} rows to Salesforce.")


class SalesforceToDF(Task):
    """
    The task for querying Salesforce and saving data as the data frame.

    Args:
        query (str, optional): Query for download the data if specific download is needed. Defaults to None.
        table (str, optional): Table name. Can be used instead of query. Defaults to None.
        columns (List[str], optional): List of columns which are needed - table argument is needed. Defaults to None.
        domain (str, optional): Domain of a connection. defaults to 'test' (sandbox).
            Can only be added if built-in username/password/security token is provided. Defaults to None.
        client_id (str, optional): Client id to keep the track of API calls. Defaults to None.
        env (str, optional): Environment information, provides information about credential
            and connection configuration. Defaults to 'DEV'.
    """

    def __init__(
        self,
        query: str = None,
        table: str = None,
        columns: List[str] = None,
        domain: str = "test",
        client_id: str = "viadot",
        env: str = "DEV",
        timeout: int = 3600,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        self.query = query
        self.table = table
        self.columns = columns
        self.domain = domain
        self.client_id = client_id
        self.env = env

        super().__init__(
            name="salesforce_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Salesforce data to a DF"""
        super().__call__(self)

    @defaults_from_attrs(
        "query",
        "table",
        "columns",
        "domain",
        "client_id",
        "env",
    )
    def run(
        self,
        query: str = None,
        table: str = None,
        columns: List[str] = None,
        env: str = None,
        domain: str = None,
        client_id: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        Task run method.

        Args:
            query (str, optional): Query for download the data if specific download is needed. Defaults to None.
            table (str, optional): Table name. Can be used instead of query. Defaults to None.
            columns (List[str], optional): List of columns which are needed - table argument is needed. Defaults to None.
            env (str, optional): Environment information, provides information about credential
                and connection configuration. Defaults to 'DEV'.
            domain (str, optional): Domain of a connection. defaults to 'test' (sandbox).
                Can only be added if built-in username/password/security token is provided. Defaults to None.
            client_id (str, optional): Client id to keep the track of API calls. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret for Salesforce. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
        """
        credentials = get_credentials(credentials_secret, vault_name=vault_name)
        salesforce = Salesforce(
            credentials=credentials,
            env=env,
            domain=domain,
            client_id=client_id,
        )
        self.logger.info(f"Retreiving the data from Salesforce...")
        df = salesforce.to_df(
            query=query, table=table, columns=columns, if_empty="replace"
        )
        self.logger.info(f"Successfully downloaded data from Salesforce.")

        return df
