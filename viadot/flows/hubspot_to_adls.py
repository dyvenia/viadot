import os
import pendulum
from typing import Any, Dict, List, Literal
from prefect import Flow

from viadot.task_utils import (
    df_to_parquet,
    add_ingestion_metadata_task,
    df_get_data_types_task,
    df_map_mixed_dtypes_for_parquet,
    update_dtypes_dict,
    dtypes_to_json_task,
)
from viadot.tasks import AzureDataLakeUpload, HubspotToDF


class HubspotToADLS(Flow):
    def __init__(
        self,
        name: str,
        endpoint: str,
        hubspot_credentials: dict,
        properties: List[Any] = [],
        filters: Dict[str, Any] = {},
        nrows: int = 1000,
        output_file_extension: str = ".parquet",
        local_file_path: str = None,
        adls_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        vault_name: str = None,
        sp_credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from Hubspot to parquet file, then uploading it to ADLS.

        Args:
            name (str): The name of the flow.
            endpoint (str): Full Hubspot endpoint ID (name) or type of schema that will be passed to the url.
            hubspot_credentials (dict): Credentials to Hubspot API.
            properties (List, optional): List of properties/columns that will be passed to the url. Defaults to [].
            filters (Dict, optional): Filters for the Hubspot API body in JSON format. Defaults to {}.
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

            nrows (int, optional): Maximum number of rows to be pulled. Defaults to 1000.
            local_file_path (str, optional): Local destination path. Defaults to None.
            output_file_extension (str, optional): Output file extension. Defaults to ".parquet".
            adls_path (str): The path to an ADLS file. Defaults to None.
            if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
            overwrite_adls (str, optional): Whether to overwrite the destination file in ADLS. Defaults to True.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
        """

        self.endpoint = endpoint
        self.properties = properties
        self.filters = filters
        self.nrows = nrows
        self.hubspot_credentials = hubspot_credentials
        self.output_file_extension = output_file_extension

        self.local_file_path = (
            local_file_path or self.slugify(name) + self.output_file_extension
        )
        self.local_json_path = self.slugify(name) + ".json"

        self.now = str(pendulum.now("utc"))

        self.adls_path = os.path.join(adls_path, self.now + self.output_file_extension)
        self.adls_schema_file_dir_file = os.path.join(
            adls_path, "schema", self.now + ".json"
        )

        self.if_exists = if_exists
        self.overwrite_adls = overwrite_adls
        self.vault_name = vault_name
        self.sp_credentials_secret = sp_credentials_secret

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

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
        dtypes_dict = df_get_data_types_task.bind(df_viadot_downloaded, flow=self)

        df_to_be_loaded = df_map_mixed_dtypes_for_parquet(
            df_viadot_downloaded, dtypes_dict, flow=self
        )

        dtypes_updated = update_dtypes_dict(dtypes_dict, flow=self)
        dtypes_to_json_task.bind(
            dtypes_dict=dtypes_updated, local_json_path=self.local_json_path, flow=self
        )
        df_to_parquet_task = df_to_parquet.bind(
            df=df_viadot_downloaded,
            path=self.local_file_path,
            if_exists="replace",
            flow=self,
        )

        file_to_adls_task = AzureDataLakeUpload()
        adls_upload = file_to_adls_task.bind(
            from_path=self.local_json_path,
            to_path=self.adls_path,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.sp_credentials_secret,
            flow=self,
        )

        json_to_adls_task = AzureDataLakeUpload()
        json_to_adls_task.bind(
            from_path=self.local_json_path,
            to_path=self.adls_schema_file_dir_file,
            overwrite=self.overwrite_adls,
            sp_credentials_secret=self.sp_credentials_secret,
            vault_name=self.vault_name,
            flow=self,
        )

        df_viadot_downloaded.set_upstream(df, flow=self)
        dtypes_dict.set_upstream(df_viadot_downloaded, flow=self)
        df_to_be_loaded.set_upstream(dtypes_dict, flow=self)
        df_to_parquet_task.set_upstream(df_to_be_loaded, flow=self)
        dtypes_to_json_task.set_upstream(df_to_parquet_task, flow=self)
        adls_upload.set_upstream(dtypes_to_json_task, flow=self)
        adls_upload.set_upstream(dtypes_to_json_task, flow=self)