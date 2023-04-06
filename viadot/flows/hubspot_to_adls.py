from typing import Any, Dict, List, Literal

from prefect import Flow

from viadot.task_utils import df_to_parquet, add_ingestion_metadata_task
from viadot.tasks import AzureDataLakeUpload, HubspotToDF


class HubspotToADLS(Flow):
    def __init__(
        self,
        name: str,
        endpoint: str,
        properties: List[Any] = [],
        filters: Dict[str, Any] = {},
        nrows: int = 1000,
        hubspot_credentials: dict = None,
        file_path: str = None,
        adls_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite: bool = True,
        vault_name: str = None,
        sp_credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from ASElite to csv file, then uploading it to ADLS.

        Args:
            name (str): The name of the flow.
            endpoint (str): Full Hubspot endpoint ID (name) or type of schema that will be passed to the url.
            properties (List, optional): List of properties/columns that will be passed to the url
            filters (Dict, optional): Filters for the Hubspot API body in JSON format. Defaults to [].
                                        - propertyName: columns for filtering
                                        - operator: operator for filtering from the list [IN, NOT_HAS_PROPERTY, LT, EQ, GT, NOT_IN, GTE, CONTAINS_TOKEN, HAS_PROPERTY, LTE, NOT_CONTAINS_TOKEN, BETWEEN, NEQ]
                                        - highValue - max value
                                        - value - min value

                                    Example below:

                                        filters = [
                                                {
                                                "filters": [
                                                    {
                                                    "propertyName": "createdate",
                                                    "operator": "BETWEEN",
                                                    "highValue": "2023-03-27",
                                                    "value": "2023-03-23"
                                                    }
                                                ]
                                                }
                                            ]

            nrows (int): Maximum number of rows to be pulled. Defaults to 1000.
            hubspot_credentials (dict): Credentials to Hubspot API. Defaults to None.
            file_path (str, optional): Local destination path. Defaults to None.
            adls_path (str): The path to an ADLS file. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            overwrite (str, optional): Whether to overwrite the destination file. Defaults to True.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
        """

        self.endpoint = endpoint
        self.properties = properties
        self.filters = filters
        self.nrows = nrows
        self.hubspot_credentials = hubspot_credentials
        self.file_path = file_path
        self.adls_path = adls_path
        self.if_exists = if_exists
        self.overwrite = overwrite
        self.vault_name = vault_name
        self.sp_credentials_secret = sp_credentials_secret

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    def gen_flow(self) -> Flow:

        hubspot_to_df_task = HubspotToDF(hubspot_credentials=self.hubspot_credentials)

        df = hubspot_to_df_task.bind(
            endpoint=self.endpoint,
            properties=self.properties,
            filters=self.filters,
            nrows=self.nrows,
            flow=self,
        )

        df_viadot_downloaded = add_ingestion_metadata_task.bind(df=df, flow=self)

        df_to_parquet_task = df_to_parquet.bind(
            df=df_viadot_downloaded, path=self.file_path, if_exists="replace", flow=self
        )

        file_to_adls_task = AzureDataLakeUpload()
        adls_upload = file_to_adls_task.bind(
            from_path=self.file_path,
            to_path=self.adls_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.sp_credentials_secret,
            flow=self,
        )

        df_viadot_downloaded.set_upstream(df, flow=self)
        df_to_parquet_task.set_upstream(df_viadot_downloaded, flow=self)
        adls_upload.set_upstream(df_to_parquet_task, flow=self)
