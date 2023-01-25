import os
from typing import Any, Dict, List, Union

import pendulum
from prefect import Flow, Task, apply_map

from viadot.task_utils import (
    add_ingestion_metadata_task,
    df_to_csv,
    df_to_parquet,
    union_dfs_task,
)
from viadot.tasks import AzureDataLakeUpload, C4CReportToDF, C4CToDF
from viadot.utils import slugify


class CloudForCustomersReportToADLS(Flow):
    def __init__(
        self,
        name: str = None,
        report_url: str = None,
        url: str = None,
        endpoint: str = None,
        params: Dict[str, Any] = {},
        fields: List[str] = None,
        skip: int = 0,
        top: int = 1000,
        channels: List[str] = None,
        months: List[str] = None,
        years: List[str] = None,
        env: str = "QA",
        c4c_credentials_secret: str = None,
        local_file_path: str = None,
        output_file_extension: str = ".csv",
        adls_dir_path: str = None,
        adls_file_path: str = None,
        overwrite_adls: bool = False,
        adls_sp_credentials_secret: str = None,
        if_empty: str = "warn",
        if_exists: str = "replace",
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from different marketing APIs to a local CSV
        using Cloud for Customers API, then uploading it to Azure Data Lake.

        Args:
            name (str): The name of the flow.
            report_url (str, optional): The url to the API. Defaults to None.
            url (str, optional): ???
            endpoint (str, optional): ???
            params (dict, optional): ???
            fields (list, optional): ???
            skip (int, optional): Initial index value of reading row. Defaults to 0.
            top (int, optional): The value of top reading row. Defaults to 1000.
            channels (List[str], optional): Filtering parameters passed to the url. Defaults to None.
            months (List[str], optional): Filtering parameters passed to the url. Defaults to None.
            years (List[str], optional): Filtering parameters passed to the url. Defaults to None.
            env (str, optional): ???
            c4c_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            username and password for the Cloud for Customers instance.
            local_file_path (str, optional): Local destination path. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of .csv for data which is not easy
            to handle with parquet. Defaults to ".csv".
            adls_dir_path (str, optional): Azure Data Lake destination folder/catalog path. Defaults to None.
            adls_file_path (str, optional): Azure Data Lake destination file path. Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite the file in ADLS. Defaults to False.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            if_empty (str, optional): What to do if the Supermetrics query returns no data. Defaults to "warn".
            if_exists (str, optional): What to do if the local file already exists. Defaults to "replace".
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """

        self.report_url = report_url
        self.skip = skip
        self.top = top
        self.if_empty = if_empty
        self.env = env
        self.c4c_credentials_secret = c4c_credentials_secret
        self.timeout = timeout

        # AzureDataLakeUpload
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.if_exists = if_exists
        self.overwrite_adls = overwrite_adls
        self.output_file_extension = output_file_extension
        self.local_file_path = (
            local_file_path or slugify(name) + self.output_file_extension
        )
        self.now = str(pendulum.now("utc"))
        self.adls_dir_path = adls_dir_path
        self.adls_file_path = adls_file_path or os.path.join(
            adls_dir_path, self.now + self.output_file_extension
        )

        # in case of non-report invoking
        self.url = url
        self.endpoint = endpoint
        self.params = params
        self.fields = fields

        # filtering report_url for reports
        self.channels = channels
        self.months = months
        self.years = years

        self.report_urls_with_filters = [self.report_url]

        self.report_urls_with_filters = self.create_url_with_fields(
            fields_list=self.channels, filter_code="CCHANNETZTEXT12CE6C2FA0D77995"
        )

        self.report_urls_with_filters = self.create_url_with_fields(
            fields_list=self.months, filter_code="CMONTH_ID"
        )

        self.report_urls_with_filters = self.create_url_with_fields(
            fields_list=self.years, filter_code="CYEAR_ID"
        )

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def create_url_with_fields(self, fields_list: List[str], filter_code: str) -> List:
        urls_list_result = []
        add_filter = True
        if len(self.report_urls_with_filters) > 1:
            add_filter = False

        if fields_list:
            for url in self.report_urls_with_filters:
                for field in fields_list:
                    if add_filter:
                        new_url = f"{url}&$filter=({filter_code}%20eq%20%27{field}%27)"
                    elif not add_filter:
                        new_url = f"{url}%20and%20({filter_code}%20eq%20%27{field}%27)"
                    urls_list_result.append(new_url)
            return urls_list_result
        else:
            return self.report_urls_with_filters

    def gen_c4c(
        self,
        url: str,
        endpoint: str,
        params: str,
        env: str,
        credentials_secret: str = None,
        flow: Flow = None,
    ) -> Task:

        c4c_to_df = C4CToDF(timeout=self.timeout)
        df = c4c_to_df.bind(
            url=url,
            endpoint=endpoint,
            params=params,
            env=env,
            credentials_secret=credentials_secret,
            flow=flow,
        )

        return df

    def gen_c4c_report_months(
        self, report_urls_with_filters: Union[str, List[str]], flow: Flow = None
    ) -> Task:

        c4c_report_to_df = C4CReportToDF(timeout=self.timeout)
        report = c4c_report_to_df.bind(
            report_url=report_urls_with_filters,
            skip=self.skip,
            top=self.top,
            env=self.env,
            credentials_secret=self.c4c_credentials_secret,
            flow=flow,
        )

        return report

    def gen_flow(self) -> Flow:
        if self.report_url:
            dfs = apply_map(
                self.gen_c4c_report_months, self.report_urls_with_filters, flow=self
            )
            df = union_dfs_task.bind(dfs, flow=self)
        elif self.url:
            df = self.gen_c4c(
                url=self.url,
                endpoint=self.endpoint,
                params=self.params,
                env=self.env,
                credentials_secret=self.c4c_credentials_secret,
                flow=self,
            )

        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )

        file_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_file_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        df_with_metadata.set_upstream(df, flow=self)
        df_to_file.set_upstream(df_with_metadata, flow=self)
        file_to_adls_task.set_upstream(df_to_file, flow=self)
