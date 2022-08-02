from typing import Any, Dict, List, Literal

from prefect import Flow, task, unmapped

from viadot.task_utils import df_to_csv
from viadot.tasks import AzureDataLakeUpload
from viadot.tasks.genesys import GenesysToDF, GenesysToCSV
from ..task_utils import (
    add_ingestion_metadata_task,
    df_to_csv,
    df_to_parquet,
)

genesys_report = GenesysToDF()
file_to_adls_task = AzureDataLakeUpload()


@task
def generate_path_list_names(file_names: List[str], adls_file_path: str) -> List[str]:
    return [adls_file_path + "/" + name for name in file_names]


class GenesysToADLS(Flow):
    def __init__(
        self,
        name: str,
        media_type_list: List[str] = None,
        queueIds_list: List[str] = None,
        data_to_post_str: str = None,
        start_date: str = None,
        end_date: str = None,
        days_interval: int = 1,
        environment: str = None,
        schedule_id: str = None,
        report_url: str = None,
        report_columns: List[str] = None,
        local_file_path: str = None,
        adls_file_path: str = None,
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """_summary_

        Args:
            name (str): The name of the Flow/Tasks.
            media_type_list (List[str], optional): List of specific media types. Defaults to None.
            queueIds_list (List[str], optional): List of specific queues ids. Defaults to None.
            data_to_post_str (str, optional): String template to generate json body. Defaults to None.
            start_date (str, optional): Start date of the report. Defaults to None.
            end_date (str, optional): End date of the report. Defaults to None.
            days_interval (int, optional): How many days report should include. Defaults to 1.
            environment (str, optional): Adress of host server. Defaults to None than will be used enviroment
            from credentials.
            schedule_id (str, optional): The ID of report. Defaults to None.
            report_url (str, optional): The url of report generated in json response. Defaults to None.
            report_columns (List[str], optional): List of exisiting column in report. Defaults to None.
            local_file_path (str, optional): The local path from which to upload the file(s). Defaults to None.
            adls_file_path (str, optional): The destination path at ADLS. Defaults to None.
            overwrite_adls (bool, optional): Whether to overwrite files in the data lake. Defaults to True.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
        """
        self.name = name
        self.media_type_list = media_type_list
        self.queueIds_list = queueIds_list
        self.data_to_post = data_to_post_str
        self.environment = environment
        self.schedule_id = schedule_id
        self.report_url = report_url
        self.report_columns = report_columns
        self.start_date = start_date
        self.end_date = end_date
        self.days_interval = days_interval
        self.local_file_path = local_file_path
        self.adls_file_path = adls_file_path
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret

        super().__init__(*args, name=name, **kwargs)

        self.genesys_task = GenesysToCSV(
            media_type_list=self.media_type_list,
            queueIds_list=self.queueIds_list,
            data_to_post_str=self.data_to_post,
            start_date=self.start_date,
            end_date=self.end_date,
            days_interval=self.days_interval,
        )

        self.gen_flow()

    def gen_flow(self) -> Flow:
        file_names = self.genesys_task.bind(
            flow=self,
            report_name=self.name,
            environment=self.environment,
            schedule_id=self.schedule_id,
            report_url=self.report_url,
            report_columns=self.report_columns,
        )

        adls_files_path = generate_path_list_names.bind(
            file_names, self.adls_file_path, flow=self
        )

        file_to_adls_task.map(
            from_path=file_names,
            to_path=adls_files_path,
            sp_credentials_secret=unmapped(self.adls_sp_credentials_secret),
            flow=self,
        )

        adls_files_path.set_upstream(file_names, flow=self)
        file_to_adls_task.set_upstream(adls_files_path, flow=self)


# ! old version
class GenesysToADLSv0(Flow):
    def __init__(
        self,
        name: str,
        columns: List[str] = None,
        vault_name: str = None,
        local_file_path: str = None,
        output_file_extension: str = ".csv",
        sep: str = "\t",
        adls_file_path: str = None,
        if_exists: Literal["replace", "append", "delete"] = "replace",
        overwrite_adls: bool = True,
        adls_sp_credentials_secret: str = None,
        credentials_secret: str = None,
        schedule_id: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        """Flow for downloading data from genesys API to Azure Data Lake in csv format by default.

        Args:
            name (str): The name of the flow.
            columns (List[str], optional): The report fields. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
            local_file_path (str, optional): Local destination path. Defaults to None.
            output_file_extension (str, optional): Output file extension - to allow selection of.csv for data
            which is not easy to handle with parquet. Defaults to ".csv".
            sep (str, optional): The separator used in csv. Defaults to "\t".
            adls_file_path (str, optional): Azure Data Lake destination file path. Defaults to None.
            if_exists (str, optional): What to do if the file exists. Defaults to "replace".
            overwrite_adls (bool, optional): Whether to overwrite files in the lake. Defaults to True.
            adls_sp_credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret for Bigquery project. Defaults to None.
            schedule_id (str, optional): ID of the schedule report job. Defaults to None.

        """

        self.name = name
        self.columns = columns
        self.vault_name = vault_name
        self.local_file_path = local_file_path
        self.output_file_extension = output_file_extension
        self.sep = sep
        self.if_exists = if_exists
        self.adls_file_path = adls_file_path
        self.overwrite_adls = overwrite_adls
        self.adls_sp_credentials_secret = adls_sp_credentials_secret
        self.credentials_secret = credentials_secret
        self.if_exsists = if_exists
        self.schedule_id = schedule_id

        super().__init__(*args, name=name, **kwargs)

        self.gen_flow()

    def gen_flow(self) -> Flow:

        df = genesys_report.bind(
            report_columns=self.columns, schedule_id=self.schedule_id, flow=self
        )
        df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)

        if self.output_file_extension == ".parquet":
            df_to_file = df_to_parquet.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                if_exists=self.if_exsists,
                flow=self,
            )
        else:
            df_to_file = df_to_csv.bind(
                df=df_with_metadata,
                path=self.local_file_path,
                sep=self.sep,
                if_exists=self.if_exsists,
                flow=self,
            )

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
