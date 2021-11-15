from typing import List
import os
import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities import logging
from ..sources import Sharepoint

logger = logging.get_logger()


class SharepointToDF(Task):
    """
    Task for converting data from Sharepoint excel file to a pandas DataFrame.

    Args:
        path_to_file (str): Path to Excel file.
        nrows (int, optional): Number of rows to read at a time. Defaults to 50000.
        skiprows (int, optional): How many rows are we skipping. Defaults to 1.
        validate_excel_file (bool, optional): Check if columns in separate sheets are the same. Defaults to False.
        if_empty (str, optional): What to do if query returns no data. Defaults to "warn".

    Returns:
        pd.DataFrame: Pandas data frame
    """

    def __init__(
        self,
        path_to_file: str = None,
        nrows: int = 50000,
        validate_excel_file: bool = False,
        if_empty: str = "warn",
        *args,
        **kwargs,
    ):

        self.if_empty = if_empty
        self.path_to_file = path_to_file
        self.nrows = nrows
        self.validate_excel_file = validate_excel_file

        super().__init__(
            name="sharepoint_to_df",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Download Sharepoint data to a DF"""
        super().__call__(self)

    def check_column_names(
        self, df_header: List = None, header_to_compare: List = None
    ) -> List:
        """
        Check if column names in sheets are the same.

        Args:
            df_header (list[str]): Header of df from excel sheet.
            header_to_compare (list[str]): Header of df from previous excel sheet.

        Returns:
            list: list of columns
        """
        df_header_list = df_header.columns.tolist()
        if header_to_compare is not None:
            if df_header_list != header_to_compare:
                logger.info("Columns in sheets are different")
                exit()
        return df_header_list

    def df_replace_special_chars(self, df: pd.DataFrame):
        return df.replace(r"\n|\t", "", regex=True)

    def split_sheet(
        self,
        sheetname: str = None,
        nrows: int = None,
        chunks: List = None,
        **kwargs,
    ) -> List[pd.DataFrame]:
        """
        Split sheet by chunks.

        Args:
            sheetname (str): The sheet on which we iterate.
            skiprows (int): How many rows are we skipping.
            nrows (int): Number of rows to read at a time.
            chunks(list): List of data in chunks.

        Returns:
            List[pd.DataFrame]: List of data frames
        """
        skiprows = 1
        logger.info(f"Worksheet: {sheetname}")
        temp_chunks = chunks
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
                df_chunk["country_code"] = sheetname
                temp_chunks.append(df_chunk)
            i_chunk += 1
        return temp_chunks

    @defaults_from_attrs(
        "path_to_file",
        "nrows",
        "validate_excel_file",
    )
    def run(
        self,
        path_to_file: str = None,
        url_to_file: str = None,
        nrows: int = 50000,
        validate_excel_file: bool = False,
        **kwargs,
    ) -> None:
        """
        Run Task ExcelToDF.

        Args:
            path_to_file (str): Path to Excel file. Defaults to None.
            url_to_file (str): Link to a file on Sharepoint. Defaults to None.
            nrows (int, optional): Number of rows to read at a time. Defaults to 50000.
            skiprows (int): How many rows are we skipping. Defaults to 1.
            validate_excel_file (bool, optional): Check if columns in separate sheets are the same. Defaults to False.

        Returns:
            pd.DataFrame: Pandas data frame
        """
        self.path_to_file = path_to_file
        self.url_to_file = url_to_file
        saved_file = os.path.basename(self.path_to_file)

        s = Sharepoint()
        s.download_file(self.url_to_file, saved_file=saved_file)

        self.nrows = nrows

        excel = pd.ExcelFile(self.path_to_file)
        header_to_compare = None
        chunks = []
        for sheetname in excel.sheet_names:
            df_header = pd.read_excel(self.path_to_file, sheet_name=sheetname, nrows=1)

            if validate_excel_file:
                header_to_compare = self.check_column_names(
                    df_header, header_to_compare
                )

            # The first row is the header.
            chunks = self.split_sheet(sheetname, self.nrows, chunks)
            df_chunks = pd.concat(chunks)

            # Rename the columns to concatenate the chunks with the header.
            columns = {i: col for i, col in enumerate(df_header.columns.tolist())}
            last_column = len(columns)
            columns[last_column] = "country_code"

            df_chunks.rename(columns=columns, inplace=True)
            df = pd.concat([df_header, df_chunks])

        df = self.df_replace_special_chars(df)
        self.logger.info(f"Successfully converted data to a DataFrame.")
        return df
