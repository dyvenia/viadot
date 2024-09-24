from typing import Any, Dict, List, Optional

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import bigquery_to_df, df_to_adls

from datetime import datetime, timedelta, date, timezone
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
             
        if (date_format=="no_sep"):
            date_str = (datetime.now(timezone.utc) - timedelta(days=int(delta))).strftime("%Y/%m/%d")   
            date_str = date_str.replace("/", "")
            modified_string = re.sub(pattern, date_str, input_string)
        
        elif (date_format=="isocalendar"):
            today = datetime.today()
            start = today - timedelta(days=7)
            week_previous = start.isocalendar()[1]
            year_previous = start.isocalendar()[0]
            date_str = datetime.fromisocalendar(year_previous, week_previous, 7).strftime("%Y/%m/%d")
            modified_string = re.sub(pattern, date_str, input_string)
            
        elif (date_format=="timestamp"):
            date_str = (datetime.now(timezone.utc) - timedelta(days=int(delta))).strftime("%Y-%m-%dT%H:%M:%S")
            modified_string = re.sub(pattern, date_str, input_string)
        
        elif (date_format=="date_obj"):
             date_obj = (datetime.now(timezone.utc) - timedelta(days=int(delta)))
             
             return date_obj

        elif (date_format=="year"):
            date_obj = (datetime.now(timezone.utc) - timedelta(days=int(delta)))
            year = str(date_obj.year)
            modified_string = re.sub(pattern_year, year, input_string)

        elif (date_format=="month"):
            date_obj = (datetime.now(timezone.utc) - timedelta(days=int(delta)))
            month = str(date_obj.month).zfill(2)
            modified_string = re.sub(pattern_month, month, input_string)
        
        elif (date_format=="day"):
            date_obj = (datetime.now(timezone.utc) - timedelta(days=int(delta)))
            day = str(date_obj.day).zfill(2)
            modified_string = re.sub(pattern_day, day, input_string)
            
        else:
            pattern_updated = fr"__delta:{delta}_format:{date_format}__"
            date_str = (datetime.now(timezone.utc) - timedelta(days=int(delta))).strftime(date_format)   
            # Replace the pattern with the actual date
            modified_string = re.sub(pattern_updated, date_str, input_string)
        
        input_string = modified_string 
            
    return input_string
        
    
@flow(
    name="BigQuery extraction to ADLS",
    description="Extract data from BigQuery and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def bigquery_to_adls(
    config_key: str = None,
    bigquery_credentials_secret: Optional[str] = None,
    query: Optional[str] = None,
    dataset_name: Optional[str] = None,
    table_name: Optional[str] = None,
    date_column_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: List[str] = [""],
    adls_config_key: Optional[str] = None,
    adls_credentials_secret: Optional[str] = None,
    adls_path: Optional[str] = None,
    overwrite: bool = True,
) -> None:
    """
    Download data from BigQuery to Azure Data Lake.

    Args:
        credentials (Optional[Dict[str, Any]], optional): Mediatool credentials as a
            dictionary. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        query (Optional[str]): SQL query to querying data in BigQuery. Format of
            basic query: (SELECT * FROM `{project}.{dataset_name}.{table_name}`).
            Defaults to None.
        dataset_name (Optional[str], optional): Dataset name. Defaults to None.
        table_name (Optional[str], optional): Table name. Defaults to None.
        dataset_name (Optional[str], optional): Dataset name. Defaults to None.
        table_name (Optional[str], optional): Table name. Defaults to None.
        date_column_name (Optional[str], optional): The user can provide the name of
            the date. If the user-specified column does not exist, all data will be
            retrieved from the table. Defaults to None.
        start_date (Optional[str], optional): Parameter to pass start date e.g.
            "2022-01-01". Defaults to None.
        end_date (Optional[str], optional): Parameter to pass end date e.g.
            "2022-01-01". Defaults to None.
        columns (List[str], optional): List of columns from given table name.
            Defaults to [""].
        adls_credentials (Optional[Dict[str, Any]], optional): The credentials as a
            dictionary. Defaults to None.
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
    
    query_updated = place_date_object_if_exist (query)
    
    data_frame = bigquery_to_df(
        config_key=config_key,
        azure_key_vault_secret=bigquery_credentials_secret,
        query=query_updated,
        dataset_name=dataset_name,
        table_name=table_name,
        date_column_name=date_column_name,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )

    adls_path_updated = place_date_object_if_exist (adls_path)

    return df_to_adls(
        df=data_frame,
        path=adls_path_updated,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite,
    )

if __name__=="__main__":
    query = f"""
        SELECT *
        FROM marketing-dev-311309.share_of_search.search_volume
        WHERE DATE(StartDate) BETWEEN '__delta:22_format:%Y-%m-%d__' AND '__delta:01_format:%Y-%m-%d__'
        """

    bigquery_to_adls(
            adls_credentials_secret= "app-azure-cr-datalakegen2-dev",
            adls_path= "raw/marketing/bigquery/google_trends_brand_interest_daily/__delta:00_format:timestamp__.parquet",
            bigquery_credentials_secret= 'bigquery-marketing-dev',
            query= query
        )