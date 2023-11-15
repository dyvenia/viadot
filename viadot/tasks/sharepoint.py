import copy
import json
import os
import re
from typing import List

import pandas as pd
from prefect import Task
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from ..exceptions import ValidationError
from ..sources import Sharepoint, SharepointList
from ..utils import add_viadot_metadata_columns
from .azure_key_vault import AzureKeyVaultSecret

logger = logging.get_logger()


class SharepointToDF(Task):
    """
    Task for converting data from Sharepoint excel file to a pandas DataFrame.

    Args:
        path_to_file (str): Path to Excel file.
        url_to_file (str):  Link to a file on Sharepoint.
                        (e.g : https://{tenant_name}.sharepoint.com/sites/{folder}/Shared%20Documents/Dashboard/file). Defaults to None.
        nrows (int, optional): Number of rows to read at a time. Defaults to 50000.
        sheet_number (int): Sheet number to be extracted from file. Counting from 0, if None all sheets are axtracted. Defaults to None.
        validate_excel_file (bool, optional): Check if columns in separate sheets are the same. Defaults to False.
        if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
        timeout(int, optional): The amount of time (in seconds) to wait while running this task before
            a timeout occurs. Defaults to 3600.

    Returns:
        pd.DataFrame: Pandas data frame
    """

    def __init__(
        self,
        path_to_file: str = None,
        url_to_file: str = None,
        nrows: int = 50000,
        sheet_number: int = None,
        validate_excel_file: bool = False,
        if_empty: str = "warn",
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        self.if_empty = if_empty
        self.path_to_file = path_to_file
        self.url_to_file = url_to_file
        self.nrows = nrows
        self.sheet_number = sheet_number
        self.validate_excel_file = validate_excel_file

        super().__init__(
            name="sharepoint_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Sharepoint data to a DF"""
        super().__call__(self)

    def check_column_names(
        self, df_header: List[str] = None, header_to_compare: List[str] = None
    ) -> List[str]:
        """
        Check if column names in sheets are the same.

        Args:
            df_header (List[str]): Header of df from excel sheet.
            header_to_compare (List[str]): Header of df from previous excel sheet.

        Returns:
            list: list of columns
        """
        df_header_list = df_header.columns.tolist()
        if header_to_compare is not None:
            if df_header_list != header_to_compare:
                raise ValidationError("Columns in sheets are different")

        return df_header_list

    def df_replace_special_chars(self, df: pd.DataFrame):
        """
        Replace "\n" and "\t" with "".

        Args:
            df (pd.DataFrame): Pandas data frame to replace characters.

        Returns:
            df (pd.DataFrame): Pandas data frame

        """
        return df.replace(r"\n|\t", "", regex=True)

    def split_sheet(
        self,
        sheetname: str = None,
        nrows: int = None,
        chunks: List[pd.DataFrame] = None,
        **kwargs,
    ) -> List[pd.DataFrame]:
        """
        Split sheet by chunks.

        Args:
            sheetname (str): The sheet on which we iterate.
            nrows (int): Number of rows to read at a time.
            chunks(List[pd.DataFrame]): List of data in chunks.

        Returns:
            List[pd.DataFrame]: List of data frames
        """
        skiprows = 1
        logger.info(f"Worksheet: {sheetname}")
        temp_chunks = copy.deepcopy(chunks)
        i_chunk = 0
        while True:
            df_chunk = pd.read_excel(
                self.path_to_file,
                sheet_name=sheetname,
                nrows=nrows,
                skiprows=skiprows,
                header=None,
                **kwargs,
            )
            skiprows += nrows
            # When there is no data, we know we can break out of the loop.
            if df_chunk.empty:
                break
            else:
                logger.debug(f" - chunk {i_chunk+1} ({df_chunk.shape[0]} rows)")
                df_chunk["sheet_name"] = sheetname
                temp_chunks.append(df_chunk)
            i_chunk += 1
        return temp_chunks

    @defaults_from_attrs(
        "path_to_file",
        "url_to_file",
        "nrows",
        "sheet_number",
        "validate_excel_file",
    )
    @add_viadot_metadata_columns(source_name="Sharepoint")
    def run(
        self,
        path_to_file: str = None,
        url_to_file: str = None,
        nrows: int = 50000,
        validate_excel_file: bool = False,
        sheet_number: int = None,
        credentials_secret: str = None,
        vault_name: str = None,
        **kwargs,
    ) -> None:
        """
        Run Task ExcelToDF.

        Args:
            path_to_file (str): Path to Excel file. Defaults to None.
            url_to_file (str): Link to a file on Sharepoint. Defaults to None.
            nrows (int, optional): Number of rows to read at a time. Defaults to 50000.
            sheet_number (int): Sheet number to be extracted from file. Counting from 0, if None all sheets are axtracted. Defaults to None.
            validate_excel_file (bool, optional): Check if columns in separate sheets are the same. Defaults to False.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET). Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secret. Defaults to None.

        Returns:
            pd.DataFrame: Pandas data frame
        """
        if not credentials_secret:
            # attempt to read a default for the service principal secret name
            try:
                credentials_secret = PrefectSecret("SHAREPOINT_KV").run()
            except ValueError:
                pass

        if credentials_secret:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials = json.loads(credentials_str)

        self.path_to_file = path_to_file
        self.url_to_file = url_to_file
        path_to_file = os.path.basename(self.path_to_file)
        self.sheet_number = sheet_number

        s = Sharepoint(download_from_path=self.url_to_file, credentials=credentials)
        s.download_file(download_to_path=path_to_file)

        self.nrows = nrows
        excel = pd.ExcelFile(self.path_to_file)

        if self.sheet_number is not None:
            sheet_names_list = [excel.sheet_names[self.sheet_number]]
        else:
            sheet_names_list = excel.sheet_names

        header_to_compare = None
        chunks = []

        for sheetname in sheet_names_list:
            df_header = pd.read_excel(self.path_to_file, sheet_name=sheetname, nrows=0)

            if validate_excel_file:
                header_to_compare = self.check_column_names(
                    df_header, header_to_compare
                )

            chunks = self.split_sheet(sheetname, self.nrows, chunks)
            df_chunks = pd.concat(chunks)

            # Rename the columns to concatenate the chunks with the header.
            columns = {i: col for i, col in enumerate(df_header.columns.tolist())}
            last_column = len(columns)
            columns[last_column] = "sheet_name"

            df_chunks.rename(columns=columns, inplace=True)
            df = pd.concat([df_header, df_chunks])

        df = self.df_replace_special_chars(df)
        self.logger.info(f"Successfully converted data to a DataFrame.")
        return df


class SharepointListToDF(Task):
    """
    Task to extract data from Sharepoint List into DataFrame.

    Args:
        list_title (str): Title of Sharepoint List. Default to None.
        site_url (str): URL to set of Sharepoint Lists. Default to None.
        required_fields (List[str]): Required fields(columns) need to be extracted from
                                        Sharepoint List. Default to None.
        field_property (List[str]): Property to expand with expand query method.
                                    All propertys can be found under list.item.properties.
                                    Default to ["Title"]
        filters (dict): Dictionary with operators which filters the SharepointList output.
                        allowed dtypes: ('datetime','date','bool','int', 'float', 'complex', 'str')
                        allowed conjuction: ('&','|')
                        allowed operators: ('<','>','<=','>=','==','!=')
                        Example how to build the dict:
                        filters = {
                        'Column_name_1' :
                                {
                                'dtype': 'datetime',
                                'value1':'YYYY-MM-DD',
                                'value2':'YYYY-MM-DD',
                                'operator1':'>=',
                                'operator2':'<=',
                                'operators_conjuction':'&',
                                'filters_conjuction':'&',
                                }
                                ,
                        'Column_name_2' :
                                {
                                'dtype': 'str',
                                'value1':'NM-PL',
                                'operator1':'==',
                                },
                        }
        row_count (int): Number of downloaded rows in single request. Default to 5000.

    Returns:
    pandas DataFrame
    """

    def __init__(
        self,
        path: str = None,
        list_title: str = None,
        site_url: str = None,
        required_fields: List[str] = None,
        field_property: str = "Title",
        filters: dict = None,
        row_count: int = 5000,
        credentials_secret: str = None,
        vault_name: str = None,
        *args,
        **kwargs,
    ):

        self.path = path
        self.list_title = list_title
        self.site_url = site_url
        self.required_fields = required_fields
        self.field_property = field_property
        self.filters = filters
        self.row_count = row_count
        self.vault_name = vault_name
        self.credentials_secret = credentials_secret

        if not credentials_secret:
            # Attempt to read a default for the service principal secret name
            try:
                credentials_secret = PrefectSecret("SHAREPOINT-CERT").run()
            except ValueError:
                pass

        if credentials_secret:
            credentials_str = AzureKeyVaultSecret(
                secret=self.credentials_secret, vault_name=self.vault_name
            ).run()
            self.credentials = json.loads(credentials_str)

        super().__init__(
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Sharepoint_List data to a .parquet file"""
        super().__call__(self)

    def _convert_camel_case_to_words(self, input_str: str) -> str:

        self.input_str = input_str

        words = re.findall(r"[A-Z][a-z]*|[0-9]+", self.input_str)
        converted = " ".join(words)

        return converted

    def change_column_name(
        self,
        df: pd.DataFrame = None,
    ):
        s = SharepointList()
        list_fields = s.get_fields(
            list_title=self.list_title,
            site_url=self.site_url,
            required_fields=self.required_fields,
        )

        self.logger.info("Changing columns names")
        column_names_correct = [field.properties["Title"] for field in list_fields]
        column_names_code = [field.properties["InternalName"] for field in list_fields]
        dictionary = dict(zip(column_names_code, column_names_correct))

        # If duplicates in names from "Title" take "InternalName"
        value_count = {}
        duplicates = []

        for key, value in dictionary.items():
            if value in value_count:
                if value_count[value] not in duplicates:
                    duplicates.append(value_count[value])
                duplicates.append(key)
            else:
                value_count[value] = key

        for key in duplicates:
            dictionary[key] = self._convert_camel_case_to_words(key)

        # Rename columns names inside DataFrame
        df = df.rename(columns=dictionary)

        return df

    def run(
        self,
    ) -> None:
        """
        Run Task SharepointListToDF.

        Returns:
            pd.DataFrame
        """

        s = SharepointList(
            credentials=self.credentials,
        )
        df_raw = s.list_item_to_df(
            list_title=self.list_title,
            site_url=self.site_url,
            required_fields=self.required_fields,
            field_property=self.field_property,
            filters=self.filters,
            row_count=self.row_count,
        )

        df = self.change_column_name(df=df_raw)
        self.logger.info("Successfully changed structure of the DataFrame")

        return df
