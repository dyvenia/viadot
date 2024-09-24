import re
import pandas as pd
import numpy as np

from typing import Any, Dict, List, Optional, Literal

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import vid_club_to_df, df_to_adls

from datetime import datetime, timedelta, date, timezone
from viadot.utils import add_viadot_metadata_columns, handle_api_response
from viadot.sources import vid_club


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
            date_obj = datetime.now(timezone.utc) - timedelta(days=int(delta))
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


def get_list_length(lst):
    if isinstance(lst, list):
        return len(lst)
    else:
        return 0


@task
def postprocess_df(df, cols_to_combine):
    for col in cols_to_combine:
        # Convert tuples to lists
        temp_col_list = df[col].apply(lambda x: list(x) if isinstance(x, tuple) else x)
        max_length = temp_col_list.apply(get_list_length).max()
        survey_column_names = []
        for i in range(0, max_length):
            list_value = f"{col}." + str(i)
            survey_column_names.append(list_value)
        df_split = df[col].apply(pd.Series)
        df_split.columns = survey_column_names
        df = pd.concat([df, df_split], axis=1)
        df = df.drop([col], axis=1)
        df = df.reset_index(drop=True)
    return df


@task
def unpack_nested_values(df, columns_to_unpack):
    for column in columns_to_unpack:
        for i in range(len(df[column])):
            if type(df[column][i]) == float:
                df[column][i] = df[column][i]
            elif type(df[column][i]) == list:
                if type(df[column][i][0]) == type(None):
                    df[column][i] = np.nan
                else:
                    try:
                        df[column][i] = int(df[column][i][0])
                    except ValueError:
                        df[column][i] = np.nan
            else:
                df[column][i] = df[column][i]
    return df


@task
def mapped_df(df, columns_mapping, mapping):
    for column in columns_mapping:
        df[column] = df[column].map(mapping)
    df.drop_duplicates(inplace=True)

    return df


@flow(
    name="Vclub extraction to ADLS",
    description="Extract data from Vclub and load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def vidclub_to_adls(
    *args: List[Any],
    source: Literal["jobs", "product", "company", "survey"] = None,
    from_date: str = "2022-03-22",
    to_date: str = None,
    items_per_page: int = 100,
    region: Literal["bg", "hu", "hr", "pl", "ro", "si", "all"] = None,
    days_interval: int = 30,
    cols_to_drop: List[str] = None,
    validate_df_dict: dict = None,
    timeout: int = 3600,
    vidclub_credentials_secret: str = "vidclub",
    cols_to_combine: list[str] = None,
    columns_to_unpack: list[str] = None,
    columns_mapping: list[str] = None,
    mapping: dict = None,
    vidclub_config_key: str = None,
    adls_config_key: Optional[str] = None,
    adls_credentials_secret: Optional[str] = None,
    adls_path: Optional[str] = None,
    adls_path_overwrite: bool = True,
) -> None:
    """
    Download data from BigQuery to Azure Data Lake.

    Args:
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
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

    from_date_updated = place_date_object_if_exist(from_date)
    to_date_updated = place_date_object_if_exist(to_date)

    df = vid_club_to_df(
        args=args,
        source=source,
        from_date=from_date_updated,
        to_date=to_date_updated,
        items_per_page=items_per_page,
        region=region,
        days_interval=days_interval,
        cols_to_drop=cols_to_drop,
        vidclub_credentials_secret=vidclub_credentials_secret,
        validate_df_dict=validate_df_dict,
        timeout=timeout,
    )

    df_derived = postprocess_df(df, cols_to_combine)
    unpacked_data = unpack_nested_values(df_derived, columns_to_unpack)
    df = mapped_df(unpacked_data, columns_mapping, mapping)

    return df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )

if __name__=="__main__":
    vidclub_to_adls(
        from_date="2022-03-22",
        to_date="__delta:01_format:%Y-%m-%d__",
        items_per_page= 100,
        source= "company",
        cols_to_combine= ["survey.7", "survey.9"],
        columns_to_unpack= ["survey.2", "survey.3", "survey.4", "survey.5", "survey.6", "survey.8"],
        mapping= {
            31: "0 Not at all likely",
            63: "1",
            57: "2",
            64: "3",
            65: "4",
            66: "5",
            67: "6",
            68: "7",
            69: "8",
            70: "9",
            32: "10 Extremely likely",
            99: "Don’t know",
            33: "VELUX representative",
            34: "VELUX Customer Support",
            35: "VELUX event",
            36: "VELUX installer magazine",
            37: "VELUX website",
            38: "VELUX e-mail or newsletter",
            39: "VELUX installer Facebook group",
            40: "Building Fair",
            41: "Local branch office",
            42: "Merchant newsletter",
            43: "Merchant website",
            28: "Other",
            103: "0-25%",
            104: "26-50%",
            105: "51-75%",
            106: "76-95%",
            107: "95%+",
            108: "1-5%",
            109: "6-10%",
            110: "11-25%",
            111: "26-50%",
            29: "More than 50%",
            99: "Don’t know",
            17: "Slope roofing",
            18: "Flat roofing",
            19: "Carpentry/joinery",
            20: "Loft conversion specialist",
            21: "Roof window installation",
            22: "Vertical window installation",
            23: "General building",
            24: "House building",
            25: "Blinds and shutters installation",
            26: "Roof window installation is a spare time job",
            27: "Roof maintenance & repair",
            28: "Other",
            56: "Self-employed",
            57: "2",
            58: "3-5",
            59: "6-10",
            60: "11-20",
            61: "21-50",
            62: "51+",
            44: "Standard windows",
            45: "Warm installation solutions (BDX, BBX)",
            46: "Roof windows replacement",
            47: "Lining preparation",
            48: "Electrical and solar INTEGRA solutions (roof windows, motors)",
            49: "Install home automatization / smart home solution",
            50: "Roof window combinations with vertical window element",
            51: "Roof terrace and balcony",
            52: "Shutters and remote controlled awning blinds",
            53: "Flat roof windows",
            54: "Sun tunnels",
            55: "Roof window repair / maintenance",
        },    
        columns_mapping= [
            "survey.2",
            "survey.4",
            "survey.5",
            "survey.6",
            "survey.8",
            "survey.7.0",
            "survey.7.1",
            "survey.9.0",
            "survey.7.2",
            "survey.9.1",
            "survey.9.2",
            "survey.9.3",
            "survey.9.4",
            "survey.9.5",
            "survey.9.6",
            "survey.9.7",
            "survey.9.8",
            "survey.9.9",
            "survey.9.10",
            "survey.9.11",
        ],
        vidclub_credentials_secret= "vidclub",
        adls_credentials_secret= "app-azure-cr-datalakegen2-dev",
        adls_path= "raw/velux_club/company.parquet"
    )