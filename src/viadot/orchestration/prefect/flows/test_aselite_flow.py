from typing import Any, Dict, List, Optional, Literal

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import aselite_to_df, df_to_adls

from datetime import datetime, timedelta, date, timezone
from dateutil.relativedelta import relativedelta
import re


@task
def place_date_object_if_exist(input_string: str):
    """_summary_
    These are the patterns that could be found in the string:
        __delta:(\d{2})_format:day__
        __delta:(\d{2})_format:month__
        __delta:(\d{2})_format:year__
        __delta:(\d{2})_format:timestamp__
        __delta:(\d{2})_format:no_sep__
        __delta:(\d{2})_format:%Y/%m/%d__
        __delta:(\d{2})_format:%Y-%m-%d__
        __delta:(\d{2})_format:%d/%m/%Y__
        __delta:(\d{2})_format:%d-%m-%Y__
    """
    pattern = r"__delta:(\d{2})_format:(.+?)__"
    pattern_day = r"__delta:(\d{2})_format:day__"
    pattern_month = r"__delta:(\d{2})_format:month__"
    pattern_year = r"__delta:(\d{2})_format:year__"

    # Check if the placeholder '__delta:01_format:no_sep__' exists in the URL
    if input_string is None:
        return None

    # Find all matches
    matches = re.findall(pattern, input_string)

    for match in matches:
        delta, date_format = match

        if date_format == "no_sep":
            date_str = (
                datetime.now(timezone.utc) - timedelta(days=int(delta))
            ).strftime("%Y/%m/%d")
            date_str = date_str.replace("/", "")
            modified_string = re.sub(pattern, date_str, input_string)

        elif date_format == "isocalendar":
            today = datetime.today()
            start = today - timedelta(days=7)
            week_previous = start.isocalendar()[1]
            year_previous = start.isocalendar()[0]
            date_str = datetime.fromisocalendar(
                year_previous, week_previous, 7
            ).strftime("%Y/%m/%d")
            modified_string = re.sub(pattern, date_str, input_string)

        elif date_format == "timestamp":
            date_str = (
                datetime.now(timezone.utc) - timedelta(days=int(delta))
            ).strftime("%Y-%m-%dT%H:%M:%S")
            modified_string = re.sub(pattern, date_str, input_string)

        elif date_format == "date_obj":
            date_obj = datetime.now(timezone.utc) - timedelta(days=int(delta))

            return date_obj

        elif date_format == "year":
            date_obj = datetime.now(timezone.utc) - relativedelta(years=int(delta))
            year = str(date_obj.year)
            modified_string = re.sub(pattern_year, year, input_string)

        elif date_format == "month":
            date_obj = datetime.now(timezone.utc) - timedelta(days=int(delta))
            month = str(date_obj.month).zfill(2)
            modified_string = re.sub(pattern_month, month, input_string)

        elif date_format == "day":
            date_obj = datetime.now(timezone.utc) - timedelta(days=int(delta))
            day = str(date_obj.day).zfill(2)
            modified_string = re.sub(pattern_day, day, input_string)

        else:
            pattern_updated = rf"__delta:{delta}_format:{date_format}__"
            date_str = (
                datetime.now(timezone.utc) - timedelta(days=int(delta))
            ).strftime(date_format)
            # Replace the pattern with the actual date
            modified_string = re.sub(pattern_updated, date_str, input_string)

        input_string = modified_string

    return input_string


@flow(
    name="ASELITE extraction to ADLS",
    description="Extract data from Aselite and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def aselite_to_adls(
    query: str = None,
    sep: str = "\t",
    file_path: str = None,
    if_exists: Literal["replace", "append", "delete"] = "replace",
    validate_df_dict: Dict[str, Any] = None,
    convert_bytes: bool = False,
    remove_special_characters: bool = None,
    columns_to_clean: List[str] = None,
    aselite_credentials_secret: str = None,
    adls_config_key: Optional[str] = None,
    adls_credentials_secret: Optional[str] = None,
    adls_path: Optional[str] = None,
    adls_path_overwrite: bool = True,
) -> None:
    """
    Download data from BigQuery to Azure Data Lake.

    Args:
        query (str): The query to be executed with pyRFC.
        sep (str, optional): The separator to use when reading query results. If not
            provided, multiple options are automatically tried. Defaults to None.
        func (str, optional): SAP RFC function to use. Defaults to None.
        replacement (str, optional): In case of sep is on a columns, set up a new
            character to replace inside the string to avoid flow breakdowns. Defaults to
            "-".
        rfc_total_col_width_character_limit (int, optional): Number of characters by
            which query will be split in chunks in case of too many columns for RFC
            function. According to SAP documentation, the limit is 512 characters.
            However, we observed SAP raising an exception even on a slightly lower
            number of characters, so we add a safety margin. Defaults to 400.
        rfc_unique_id (list[str], optional):
            Reference columns to merge chunks DataFrames. These columns must to be
            unique. If no columns are provided in this parameter, all data frame columns
            will by concatenated. Defaults to None.
        tests (dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from viadot.utils. Defaults to None.
        alternative_version (bool, optional): Enable the use version 2 in source.
            Defaults to False.
        saprfc_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        saprfc_credentials_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        adls_config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (Optional[str], optional): Azure Data Lake destination file path.
            Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    df = aselite_to_df(
        query=query,
        credentials_secret=aselite_credentials_secret,
        sep=sep,
        file_path=file_path,
        if_exists=if_exists,
        validate_df_dict=validate_df_dict,
        convert_bytes=convert_bytes,
        remove_special_characters=remove_special_characters,
        columns_to_clean=columns_to_clean,
    )

    adls_path_updated = place_date_object_if_exist(adls_path)

    return df_to_adls(
        df=df,
        path=adls_path_updated,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )
    
    
if __name__=="__main__":
    query = """SELECT [ActivityKey]
        ,[ProspectKey]
        ,[Subject] 
        ,[ActivityOwner] 
        ,[ActiveStatus] 
        ,[NotificationEmail]
        ,[DueDateStart]
        ,[DueDateEnd]
        ,[Direction Name]
        ,[Type]
        ,[TransactionType_Code] 
        ,[Ticket ID] 
        ,[Task ID] 
        ,[Category2]
        ,[Meeting type MASK] 
        ,[CompanyCode]
        ,[Telephone Call Topic]
        ,[GenesysID]
        ,[No. of Participants]       
  FROM [UCRMPROD].[dbo].[CRM_2_BO_Activities]"""

    aselite_to_adls(
        query= query,
        adls_credentials_secret= "app-azure-cr-datalakegen2-dev",
        adls_path= "raw/aselite/CRM_2_BO_Activities.csv",
        aselite_credentials_secret= "aselite"
    )