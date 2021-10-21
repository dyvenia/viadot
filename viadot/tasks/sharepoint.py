from datetime import timedelta
from typing import Any, Dict, List, Union
import openpyxl
import os

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..sources import Sharepoint

s = Sharepoint()

class SharepointToDF(Task):
    """
    Task for converting data from Sharepoint excel file to a pandas DataFrame.

    Args:
        path_to_file (str): Path to Excel file.
        nrows (int, optional): Number of rows to read at a time. Defaults to 10000.
        check_col_names (bool, optional): Check if columns in separate sheets are the same. Defaults to False.
        if_empty (str, optional): What to do if query returns no data. Defaults to "warn".
    
    Returns:
        pd.dataframe: Data frame
    """
    def __init__(
        self,
        path_to_file: str = None,
        nrows: int = 10000,
        check_col_names: bool = False,
        if_empty: str = "warn",
        *args,
        **kwargs,
    ):

        self.if_empty = if_empty
        self.path_to_file = path_to_file
        self.nrows = nrows
        self.check_col_names = check_col_names

        super().__init__(
            name="excel_to_df",
            *args,
            **kwargs,
        )

    # def __call__(self):
    #     """Download Sharepoint data to a DF"""
    #     super().__call__(self)


    def check_column_names(self, df_header, header_to_compare):
        """
        Check if column names in sheets are the same.

        Args:
            df_header (list[str]): Header of df from excel sheet.
            header_to_compare (list[str]): Header of df from previous excel sheet.

        Returns:
            list: list of columns
        """		
        df_header_list = df_header.columns.tolist()
        if header_to_compare != None:
            if df_header_list != header_to_compare:
                print('[!] Columns in sheets are different')
                exit()
        return df_header_list


    def split_sheet(self, sheetname, skiprows, nrows, chunks, **kwargs):
        """
        Split sheet by chunks.

        Args:
            sheetname (str): The sheeet on which we iterate.
            skiprows (int): How many rows are we skipping.
            nrows (int): How many rows does our chunk have.

        Returns:
            List[df]: List of data frames
        """        
        print(f"Worksheet: {sheetname}")
        # chunks = []
        i_chunk = 0
        while True:
            df_chunk = pd.read_excel(
                self.path_to_file, 
                sheet_name=sheetname,
                nrows=nrows, 
                skiprows=skiprows, 
                header=None, 
                **kwargs
                )
            skiprows += nrows
            # When there is no data, we know we can break out of the loop.
            if not df_chunk.shape[0]:
                break
            else:
                print(f" - chunk {i_chunk+1} ({df_chunk.shape[0]} rows)")
                chunks.append(df_chunk)
            i_chunk += 1
        return chunks


    def run(
        self,
        path_to_file: str = None,
        nrows: int = 10000,
        check_col_names: bool = False,
        **kwargs
        ):
        """
        Run Task ExcelToDF.

        Args:
            path_to_file (str): Path to Excel file.
            nrows (int, optional): Number of rows to read at a time. Defaults to 10000.
            check_col_names (bool, optional): Check if columns in separate sheets are the same. Defaults to False.

        Returns:
            pd.DataFrame: Data frame
        """	
        self.path_to_file = path_to_file
        
        # pobieranie pliku
        filename = os.path.basename(self.path_to_file)
        s.download_file(filename=filename)

        ## ## ##
    
        self.nrows = nrows

        xl = pd.ExcelFile(self.path_to_file)
        header_to_compare = None
        chunks = []
        for sheetname in xl.sheet_names:
            df_header = pd.read_excel(self.path_to_file, sheet_name=sheetname, nrows=1)

            if check_col_names:
                header_to_compare = self.check_column_names(df_header,header_to_compare)

            # The first row is the header.
            skiprows = 1
            chunks = self.split_sheet(sheetname, skiprows, self.nrows, chunks)
            df_chunks = pd.concat(chunks)

            # Rename the columns to concatenate the chunks with the header.
            columns = {i: col for i, col in enumerate(df_header.columns.tolist())}
            df_chunks.rename(columns=columns, inplace=True)
            df = pd.concat([df_header, df_chunks])
            

        self.logger.info(f"Successfully converted data to a DataFrame.")
        return df

