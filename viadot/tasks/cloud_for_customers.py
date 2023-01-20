import json
from datetime import timedelta
from typing import Dict, Generator, List

import pandas as pd
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.tasks import defaults_from_attrs

from viadot.config import local_config

from ..sources import CloudForCustomers
from .azure_key_vault import AzureKeyVaultSecret


class C4CReportToDF(Task):
    def __init__(
        self,
        *args,
        report_url: str = None,
        skip: int = 0,
        top: int = 1000,
        env: str = "QA",
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 3600,
        **kwargs,
    ):

        self.report_url = report_url
        self.env = env
        self.skip = skip
        self.top = top

        super().__init__(
            name="c4c_report_to_df",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self, *args, **kwargs):
        """Download report to DF"""
        return super().__call__(*args, **kwargs)

    @defaults_from_attrs(
        "report_url",
        "env",
        "skip",
        "top",
    )
    def run(
        self,
        report_url: str = None,
        env: str = "QA",
        skip: int = 0,
        top: int = 1000,
        credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
    ):
        """
        Task for downloading data from the Cloud for Customers to a pandas DataFrame using report URL.

        Args:
            report_url (str, optional): The URL to the report. Defaults to None.
            env (str, optional): The environment to use. Defaults to 'QA'.
            skip (int, optional): Initial index value of reading row. Defaults to 0.
            top (int, optional): The value of top reading row. Defaults to 1000.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with C4C credentials (username & password). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

        Returns:
            pd.DataFrame: The query result as a pandas DataFrame.
        """

        if not credentials_secret:
            try:
                credentials_secret = PrefectSecret("C4C_KV").run()
            except ValueError:
                pass

        if credentials_secret:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials = json.loads(credentials_str)
        else:
            credentials = local_config.get("CLOUD_FOR_CUSTOMERS")[env]

        final_df = pd.DataFrame()
        next_batch = True
        while next_batch:
            new_url = f"{report_url}&$top={top}&$skip={skip}"
            chunk_from_url = CloudForCustomers(
                report_url=new_url, env=env, credentials=credentials
            )
            df = chunk_from_url.to_df()
            final_df = final_df.append(df)
            if not final_df.empty:
                df_count = df.count()[1]
                if df_count != top:
                    next_batch = False
                skip += top
            else:
                break
        return final_df


class C4CToDF(Task):
    def __init__(
        self,
        *args,
        url: str = None,
        endpoint: str = None,
        fields: List[str] = None,
        params: Dict[str, str] = None,
        chunksize: int = 20000,
        env: str = "QA",
        if_empty: str = "warn",
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        timeout: int = 3600,
        **kwargs,
    ):

        self.url = url
        self.endpoint = endpoint
        self.fields = fields
        self.params = params
        self.chunksize = chunksize
        self.env = env
        self.if_empty = if_empty

        super().__init__(
            name="c4c_to_df",
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "url", "endpoint", "fields", "params", "chunksize", "env", "if_empty"
    )
    def run(
        self,
        url: str = None,
        env: str = "QA",
        endpoint: str = None,
        fields: List[str] = None,
        params: Dict[str, str] = None,
        chunksize: int = None,
        if_empty: str = "warn",
        credentials_secret: str = None,
        vault_name: str = None,
    ):
        """
        Task for downloading data from the Cloud for Customers to a pandas DataFrame using normal URL (with query parameters).
        This task grab data from table from 'scratch' with passing table name in url or endpoint. It is rocommended to add
        some filters parameters in this case.

        Example:
            url = "https://mysource.com/sap/c4c/odata/v1/c4codataapi"
            endpoint = "ServiceRequestCollection"
            params = {"$filter": "CreationDateTime ge 2021-12-21T00:00:00Z"}

        Args:
            url (str, optional): The url to the API in case of prepared report. Defaults to None.
            env (str, optional): The environment to use. Defaults to 'QA'.
            endpoint (str, optional): The endpoint of the API. Defaults to None.
            fields (List[str], optional): The C4C Table fields. Defaults to None.
            params (Dict[str, str]): Query parameters. Defaults to $format=json.
            chunksize (int, optional): How many rows to retrieve from C4C at a time. Uses a server-side cursor.
            if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary
            with C4C credentials. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

        Returns:
            pd.DataFrame: The query result as a pandas DataFrame.
        """
        if not credentials_secret:
            try:
                credentials_secret = PrefectSecret("C4C_KV").run()
            except ValueError:
                pass

        if credentials_secret:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials = json.loads(credentials_str)
        else:
            credentials = local_config.get("CLOUD_FOR_CUSTOMERS")[env]

        self.logger.info(f"Downloading data from {url+endpoint}...")

        # If we get any of these in params, we don't perform any chunking
        if any(["$skip" in params, "$top" in params]):
            return CloudForCustomers(
                url=url,
                endpoint=endpoint,
                params=params,
                env=env,
                credentials=credentials,
            ).to_df(if_empty=if_empty, fields=fields)

        def _generate_chunks() -> Generator[pd.DataFrame, None, None]:
            """
            Util returning chunks as a generator to save memory.
            """
            offset = 0
            total_record_count = 0
            while True:
                boundaries = {"$skip": offset, "$top": chunksize}
                params.update(boundaries)

                chunk = CloudForCustomers(
                    url=url,
                    endpoint=endpoint,
                    params=params,
                    env=env,
                    credentials=credentials,
                ).to_df(if_empty=if_empty, fields=fields)

                chunk_record_count = chunk.shape[0]
                total_record_count += chunk_record_count
                self.logger.info(
                    f"Successfully downloaded {total_record_count} records."
                )

                yield chunk

                if chunk.shape[0] < chunksize:
                    break

                offset += chunksize

        self.logger.info(f"Data from {url+endpoint} has been downloaded successfully.")

        chunks = _generate_chunks()
        df = pd.concat(chunks)

        return df
