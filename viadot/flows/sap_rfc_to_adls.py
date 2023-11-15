from typing import Any, Dict, List, Literal

from prefect import Flow

from viadot.task_utils import df_to_csv, df_to_parquet, set_new_kv, validate_df
from viadot.tasks import AzureDataLakeUpload, SAPRFCToDF


class SAPRFCToADLS(Flow):
    def __init__(
        self,
        name: str,
        query: str = None,
        rfc_sep: str = None,
        rfc_replacement: str = "-",
        func: str = "RFC_READ_TABLE",
        rfc_total_col_width_character_limit: int = 400,
        rfc_unique_id: List[str] = None,
        sap_credentials: dict = None,
        output_file_extension: str = ".parquet",
        local_file_path: str = None,
        file_sep: str = "\t",
        if_exists: Literal["append", "replace", "skip"] = "replace",
        adls_path: str = None,
        overwrite: bool = False,
        adls_sp_credentials_secret: str = None,
        vault_name: str = None,
        update_kv: bool = False,
        filter_column: str = None,
        alternative_version: bool = False,
        validate_df_dict: Dict[str, Any] = None,
        timeout: int = 3600,
        *args: List[any],
        **kwargs: Dict[str, Any],
    ):
        """
        Flow for downloading data from SAP database using the RFC protocol and uploading it to Azure Data Lake.

        Note that only a very limited subset of SQL is supported:
            - aliases
            - where clauses combined using the AND operator
            - limit & offset

        Unsupported:
            - aggregations
            - joins
            - subqueries
            - etc.

        Args:
            name (str): The name of the flow.
            query (str): Query to be executed with pyRFC. Defaults to None.
            rfc_sep(str, optional): Which separator to use when querying SAP. If not provided, multiple options are automatically tried.
            rfc_replacement (str, optional): In case of sep is on a columns, set up a new character to replace
                inside the string to avoid flow breakdowns. Defaults to "-".
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            rfc_total_col_width_character_limit (int, optional): Number of characters by which query will be split in chunks in case of too many columns
                for RFC function. According to SAP documentation, the limit is 512 characters. However, we observed SAP raising an exception
                even on a slightly lower number of characters, so we add a safety margin. Defaults to 400.
            rfc_unique_id  (List[str], optional): Reference columns to merge chunks Data Frames. These columns must to be unique. If no columns are provided
                in this parameter, all data frame columns will by concatenated. Defaults to None.
                Example:
                --------
                SAPRFCToADLS(
                    ...
                    rfc_unique_id=["VBELN", "LPRIO"],
                    ...
                    )
            sap_credentials (dict, optional): The credentials to use to authenticate with SAP. By default, they're taken from the local viadot config.
            output_file_extension (str, optional): Output file extension - to allow selection of .csv for data which is not easy to handle with parquet. Defaults to ".parquet".
            local_file_path (str, optional): Local destination path. Defaults to None.
            file_sep(str, optional): The separator to use in the CSV. Defaults to "\t".
            if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace".
            adls_path(str, optional): Azure Data Lake destination file path. Defaults to None.
            overwrite(bool, optional) Whether to overwrite the file in ADLS. Defaults to False.
            adls_sp_credentials_secret(str, optional): The name of the Azure Key Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.Defaults to None.
            vault_name(str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            update_kv (bool, optional): Whether or not to update key value on Prefect. Defaults to False.
            filter_column (str, optional): Name of the field based on which key value will be updated. Defaults to None.
            alternative_version (bool, optional): Enable the use version 2 in source. Defaults to False.
            validate_df_dict (Dict[str,Any], optional): A dictionary with optional list of tests to verify the output dataframe. If defined, triggers
                the `validate_df` task from task_utils. Defaults to None.
            timeout(int, optional): The amount of time (in seconds) to wait while running this task before
                a timeout occurs. Defaults to 3600.
        """
        self.query = query
        self.rfc_sep = rfc_sep
        self.rfc_replacement = rfc_replacement
        self.func = func
        self.rfc_total_col_width_character_limit = rfc_total_col_width_character_limit
        self.rfc_unique_id = rfc_unique_id
        self.sap_credentials = sap_credentials
        self.output_file_extension = output_file_extension
        self.local_file_path = local_file_path
        self.file_sep = file_sep
        self.if_exists = if_exists
        self.adls_path = adls_path
        self.overwrite = overwrite
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.vault_name = vault_name
        self.alternative_version = alternative_version
        self.validate_df_dict = validate_df_dict
        self.timeout = timeout

        self.update_kv = update_kv
        self.filter_column = filter_column

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:
        download_sap_task = SAPRFCToDF(timeout=self.timeout)
        df = download_sap_task(
            query=self.query,
            sep=self.rfc_sep,
            replacement=self.rfc_replacement,
            func=self.func,
            rfc_total_col_width_character_limit=self.rfc_total_col_width_character_limit,
            rfc_unique_id=self.rfc_unique_id,
            alternative_version=self.alternative_version,
            credentials=self.sap_credentials,
            flow=self,
        )
        if self.validate_df_dict:
            validation_task = validate_df.bind(
                df, tests=self.validate_df_dict, flow=self
            )
            validation_task.set_upstream(df, flow=self)

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df,
                sep=self.file_sep,
                path=self.local_file_path,
                if_exists=self.if_exists,
                flow=self,
            )

        file_to_adls_task = AzureDataLakeUpload(timeout=self.timeout)
        adls_upload = file_to_adls_task.bind(
            from_path=self.local_file_path,
            to_path=self.adls_path,
            overwrite=self.overwrite,
            sp_credentials_secret=self.adls_sp_credentials_secret,
            flow=self,
        )

        if self.validate_df_dict:
            df_to_file.set_upstream(validation_task, flow=self)

        df_to_file.set_upstream(df, flow=self)
        adls_upload.set_upstream(df_to_file, flow=self)

        if self.update_kv == True:
            set_new_kv.bind(
                kv_name=self.name,
                df=df,
                filter_column=self.filter_column,
                flow=self,
            )
            set_new_kv.set_upstream(adls_upload, flow=self)
