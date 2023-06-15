import re
from collections import OrderedDict
from typing import List, Literal
from typing import OrderedDict as OrderedDictType
from typing import Tuple, Union

import numpy as np
import pandas as pd
from prefect.utilities import logging

try:
    import pyrfc
    from pyrfc._exception import ABAPApplicationError
except ModuleNotFoundError:
    raise ImportError("pyfrc is required to use the SAPRFC source.")
from sql_metadata import Parser

from viadot.config import local_config
from viadot.exceptions import CredentialError, DataBufferExceeded
from viadot.sources.base import Source

logger = logging.get_logger()


def remove_whitespaces(text):
    return " ".join(text.split())


def get_keyword_for_condition(where: str, condition: str) -> str:
    where = where[: where.find(condition)]
    return where.split()[-1]


def get_where_uppercased(where: str) -> str:
    """
    Uppercase a WHERE clause's keywords without
    altering the original string.
    """
    where_and_uppercased = re.sub("\\sand ", " AND ", where)
    where_and_and_or_uppercased = re.sub("\\sor ", " OR ", where_and_uppercased)
    return where_and_and_or_uppercased


def remove_last_condition(where: str) -> str:
    """Remove the last condtion from a WHERE clause."""
    where = get_where_uppercased(where)
    split_by_and = re.split("\\sAND ", where)
    conditions = [re.split("\\sOR ", expr) for expr in split_by_and]
    conditions_flattened = [
        condition for sublist in conditions for condition in sublist
    ]

    condition_to_remove = conditions_flattened[-1]

    where_trimmed = where[: where.find(condition_to_remove)].split()
    where_trimmed_without_last_keyword = " ".join(where_trimmed[:-1])

    return where_trimmed_without_last_keyword, condition_to_remove


def trim_where(where: str) -> Tuple[str, OrderedDictType[str, str]]:
    """
    Trim a WHERE clause to 75 characters or less,
    as required by SAP. The rest of filters will be applied
    in-memory on client side.
    """

    if len(where) <= 75:
        return where, None

    wheres_to_add = OrderedDict()
    keywords_with_conditions = []
    where_trimmed = where
    while len(where_trimmed) > 75:
        # trim the where
        where_trimmed, removed_condition = remove_last_condition(where_trimmed)

        # store the removed conditions so we can readd them later
        keyword = get_keyword_for_condition(where, removed_condition)
        keywords_with_conditions.append((keyword, removed_condition))

    wheres_to_add_sorted = keywords_with_conditions[::-1]
    wheres_to_add = OrderedDict(wheres_to_add_sorted)

    return where_trimmed, wheres_to_add


def detect_extra_rows(
    row_index: int, data_raw: np.array, chunk: int, fields: List[str]
) -> Union[int, np.array, bool]:
    """Check if, in between calls to the SAP table, the number of rows have increased.
        If so, remove the last rows added, to fit the size of the previous columns.

    Args:
        row_index (int): Number of rows set it down in he first iteration with the SAP table.
        data_raw (np.array): Array with the data retrieve from SAP table.
        chunk (int): The part number in which a number of SAP table columns have been split.
        fields (List[str]): A list with the names of the columns in a chunk.

    Returns:
        Union[int, np.array, bool]: A tuple with the parameters "row_index", "data_raw", a new
            boolean variable "start" to indicate when the for loop has to be restarted,
            and "chunk" variable.
    """
    start = False
    if row_index == 0:
        row_index = data_raw.shape[0]
        if row_index == 0:
            logger.warning(
                f"Empty output was generated for chunk {chunk} in columns {fields}."
            )
            start = True
    elif data_raw.shape[0] != row_index:
        data_raw = data_raw[:row_index]
        logger.warning(
            f"New rows were generated during the execution of the script. The table is truncated to the number of rows for the first chunk."
        )

    return row_index, data_raw, start


def replace_separator_in_data(
    data_raw: np.array,
    no_sep_index: np.array,
    record_key: str,
    pos_sep_index: np.array,
    sep: str,
    replacement: str,
) -> np.array:
    """Function to replace the extra separator in every row of the data_raw numpy array.

    Args:
        data_raw (np.array): Array with the data retrieve from SAP table.
        no_sep_index (np.array): Array with indexes where are extra separators characters in rows.
        record_key (str): Key word to extract the data from the numpy array "data_raw".
        pos_sep_index (np.array): Array with indexes where are placed real separators.
        sep (str): Which separator to use when querying SAP.
        replacement (str): In case of sep is on a columns, set up a new character to replace
            inside the string to avoid flow breakdowns.

    Returns:
        np.array: the same data_raw numpy array with the "replacement" separator instead.
    """
    for no_sep in no_sep_index:
        logger.warning(
            "A separator character was found and replaced inside a string text that could produce future errors:"
        )
        logger.warning("\n" + data_raw[no_sep][record_key])
        split_array = np.array([*data_raw[no_sep][record_key]])
        position = np.where(split_array == f"{sep}")[0]
        index_sep_index = np.argwhere(np.in1d(position, pos_sep_index) == False)
        index_sep_index = index_sep_index.reshape(
            len(index_sep_index),
        )
        split_array[position[index_sep_index]] = replacement
        data_raw[no_sep][record_key] = "".join(split_array)
        logger.warning("\n" + data_raw[no_sep][record_key])

    return data_raw


def catch_extra_separators(
    data_raw: np.array, record_key: str, sep: str, fields: List[str], replacement: str
) -> np.array:
    """Function to replace extra separators in every row of the table.

    Args:
        data_raw (np.array): Array with the data retrieve from SAP table.
        record_key (str): Key word to extract the data from the numpy array "data_raw".
        sep (str): Which separator to use when querying SAP.
        fields (List[str]): A list with the names of the columns in a chunk.
        replacement (str): In case of sep is on a columns, set up a new character to replace
            inside the string to avoid flow breakdowns.

    Returns:
        np.array: The argument "data_raw" with no extra delimiters.
    """

    # remove scape characters from data_raw ("\t")
    for n, r in enumerate(data_raw):
        if "\t" in r[record_key]:
            data_raw[n][record_key] = r[record_key].replace("\t", " ")

    # first it is identified where the data has an extra separator in text columns.
    sep_counts = np.array([], dtype=int)
    for row in data_raw:
        sep_counts = np.append(sep_counts, row[record_key].count(f"{sep}"))

    no_sep_index = np.argwhere(sep_counts != len(fields) - 1)
    no_sep_index = no_sep_index.reshape(
        len(no_sep_index),
    )
    sep_index = np.argwhere(sep_counts == len(fields) - 1)
    sep_index = sep_index.reshape(
        len(sep_index),
    )
    # indentifying "good" rows we obtain the index of separator positions.
    pos_sep_index = np.array([], dtype=int)
    for data in data_raw[sep_index]:
        pos_sep_index = np.append(
            pos_sep_index,
            np.where(np.array([*data[record_key]]) == f"{sep}"),
        )
    pos_sep_index = np.unique(pos_sep_index)

    # in rows with an extra separator, we replace them by another character: "-" by default
    data_raw = replace_separator_in_data(
        data_raw,
        no_sep_index,
        record_key,
        pos_sep_index,
        sep,
        replacement,
    )

    return data_raw


class SAPRFC(Source):
    """
    A class for querying SAP with SQL using the RFC protocol.

    Note that only a very limited subset of SQL is supported:
    - aliases
    - where clauses combined using the AND operator
    - limit & offset

    Unsupported:
    - aggregations
    - joins
    - subqueries
    - etc.
    """

    def __init__(
        self,
        sep: str = None,
        func: str = "RFC_READ_TABLE",
        rfc_total_col_width_character_limit: int = 400,
        *args,
        **kwargs,
    ):
        """Create an instance of the SAPRFC class.

        Args:
            sep (str, optional): Which separator to use when querying SAP. If not provided,
            multiple options are automatically tried.
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            rfc_total_col_width_character_limit (int, optional): Number of characters by which query will be split in chunks
            in case of too many columns for RFC function. According to SAP documentation, the limit is
            512 characters. However, we observed SAP raising an exception even on a slightly lower number
            of characters, so we add a safety margin. Defaults to 400.

        Raises:
            CredentialError: If provided credentials are incorrect.
        """

        self._con = None
        DEFAULT_CREDENTIALS = local_config.get("SAP").get("DEV")
        credentials = kwargs.pop("credentials", None) or DEFAULT_CREDENTIALS
        if credentials is None:
            raise CredentialError("Missing credentials.")

        super().__init__(*args, credentials=credentials, **kwargs)

        self.sep = sep
        self.client_side_filters = None
        self.func = func
        self.rfc_total_col_width_character_limit = rfc_total_col_width_character_limit

    @property
    def con(self) -> pyrfc.Connection:
        if self._con is not None:
            return self._con
        con = pyrfc.Connection(**self.credentials)
        self._con = con
        return con

    def check_connection(self) -> None:
        self.logger.info("Checking the connection...")
        self.con.ping()
        self.logger.info("Connection has been validated successfully.")

    def close_connection(self) -> None:
        """Closing RFC connection."""
        self.con.close()
        self.logger.info("Connection has been closed successfully.")

    def get_function_parameters(
        self,
        function_name: str,
        description: Union[None, Literal["short", "long"]] = "short",
        *args,
    ) -> Union[List[str], pd.DataFrame]:
        """Get the description for a SAP RFC function.

        Args:
            function_name (str): The name of the function to detail.
            description (Union[None, Literal[, optional): Whether to display
            a short or a long description. Defaults to "short".

        Raises:
            ValueError: If the argument for description is incorrect.

        Returns:
            Union[List[str], pd.DataFrame]: Either a list of the function's
            parameter names (if 'description' is set to None),
            or a short or long description.
        """
        if description is not None:
            if description not in ["short", "long"]:
                raise ValueError(
                    "Incorrect value for 'description'. Correct values: (None, 'short', 'long'"
                )

        descr = self.con.get_function_description(function_name, *args)
        param_names = [param["name"] for param in descr.parameters]
        detailed_params = descr.parameters
        filtered_detailed_params = [
            {
                "name": param["name"],
                "parameter_type": param["parameter_type"],
                "default_value": param["default_value"],
                "optional": param["optional"],
                "parameter_text": param["parameter_text"],
            }
            for param in descr.parameters
        ]

        if description is not None:
            if description == "long":
                params = detailed_params
            else:
                params = filtered_detailed_params
            params = pd.DataFrame.from_records(params)
        else:
            params = param_names

        return params

    def _get_where_condition(self, sql: str) -> str:
        """Retrieve the WHERE conditions from a SQL query.

        Args:
            sql (str): The input SQL query.

        Raises:
            ValueError: Raised if the WHERE clause is longer than
            75 characters (SAP's limitation) and the condition for the
            extra clause(s) is OR.

        Returns:
            str: The where clause trimmed to <= 75 characters.
        """

        where_match = re.search("\\sWHERE ", sql.upper())
        if not where_match:
            return None

        limit_match = re.search("\\sLIMIT ", sql.upper())
        limit_pos = limit_match.span()[0] if limit_match else len(sql)

        where = sql[where_match.span()[1] : limit_pos]
        where_sanitized = remove_whitespaces(where)
        where_trimmed, client_side_filters = trim_where(where_sanitized)
        if client_side_filters:
            self.logger.warning(
                "A WHERE clause longer than 75 character limit detected."
            )
            if "OR" in [key.upper() for key in client_side_filters.keys()]:
                raise ValueError(
                    "WHERE conditions after the 75 character limit can only be combined with the AND keyword."
                )
            else:
                filters_pretty = list(client_side_filters.items())
                self.logger.warning(
                    f"Trimmed conditions ({filters_pretty}) will be applied client-side."
                )
                self.logger.warning(f"See the documentation for caveats.")

        self.client_side_filters = client_side_filters
        return where_trimmed

    @staticmethod
    def _get_table_name(sql: str) -> str:
        parsed = Parser(sql)
        if len(parsed.tables) > 1:
            raise ValueError("Querying more than one table is not supported.")
        return parsed.tables[0]

    def _build_pandas_filter_query(
        self, client_side_filters: OrderedDictType[str, str]
    ) -> str:
        """Build a WHERE clause that will be applied client-side.
        This is required if the WHERE clause passed to query() is
        longer than 75 characters.

        Args:
            client_side_filters (OrderedDictType[str, str]): The
            client-side filters to apply.

        Returns:
            str: the WHERE clause reformatted to fit the format
            required by DataFrame.query().
        """
        for i, f in enumerate(client_side_filters.items()):
            if i == 0:
                # skip the first keyword; we assume it's "AND"
                query = f[1]
            else:
                query += " " + f[0] + " " + f[1]

            filter_column_name = f[1].split()[0]
            resolved_column_name = self._resolve_col_name(filter_column_name)
        query = re.sub("\\s?=\\s?", " == ", query).replace(
            filter_column_name, resolved_column_name
        )
        return query

    def extract_values(self, sql: str) -> None:
        """TODO: This should cover all values, not just columns"""
        self.where = self._get_where_condition(sql)
        self.select_columns = self._get_columns(sql, aliased=False)
        self.select_columns_aliased = self._get_columns(sql, aliased=True)

    def _resolve_col_name(self, column: str) -> str:
        """Get aliased column name if it exists, otherwise return column name."""
        return self.aliases_keyed_by_columns.get(column, column)

    def _get_columns(self, sql: str, aliased: bool = False) -> List[str]:
        """Retrieve column names from a SQL query.

        Args:
            sql (str): The SQL query to parse.
            aliased (bool, optional): Whether to returned aliased
            names. Defaults to False.

        Returns:
            List[str]: A list of column names.
        """
        parsed = Parser(sql)
        columns = list(parsed.columns_dict["select"])
        if aliased:
            aliases_keyed_by_alias = parsed.columns_aliases
            aliases_keyed_by_columns = OrderedDict(
                {val: key for key, val in aliases_keyed_by_alias.items()}
            )

            self.aliases_keyed_by_columns = aliases_keyed_by_columns

            columns = [
                aliases_keyed_by_columns[col]
                if col in aliases_keyed_by_columns
                else col
                for col in columns
            ]

        if self.client_side_filters:
            # In case the WHERE clause is > 75 characters long, we execute the rest of the filters
            # client-side. To do this, we need to pull all fields in the client-side WHERE conditions.
            # Below code adds these columns to the list of SELECTed fields.
            cols_to_add = [v.split()[0] for v in self.client_side_filters.values()]
            if aliased:
                cols_to_add = [aliases_keyed_by_columns[col] for col in cols_to_add]
            columns.extend(cols_to_add)
            columns = list(dict.fromkeys(columns))  # remove duplicates

        return columns

    @staticmethod
    def _get_limit(sql: str) -> int:
        """Get limit from the query"""
        limit_match = re.search("\\sLIMIT ", sql.upper())
        if not limit_match:
            return None

        return int(sql[limit_match.span()[1] :].split()[0])

    @staticmethod
    def _get_offset(sql: str) -> int:
        """Get offset from the query"""
        offset_match = re.search("\\sOFFSET ", sql.upper())
        if not offset_match:
            return None

        return int(sql[offset_match.span()[1] :].split()[0])

    def query(self, sql: str, sep: str = None) -> None:
        """Parse an SQL query into pyRFC commands and save it into
        an internal dictionary.

        Args:
            sql (str): The SQL query to be ran.
            sep (str, optional): The separator to be used
            to split columns in the result blob. Defaults to self.sep.

        Raises:
            ValueError: If the query is not a SELECT query.
        """

        if not sql.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT queries are supported.")

        sep = sep if sep is not None else self.sep

        self.sql = sql

        self.extract_values(sql)

        table_name = self._get_table_name(sql)
        # this has to be called before checking client_side_filters
        where = self.where
        columns = self.select_columns
        character_limit = self.rfc_total_col_width_character_limit
        # due to the RFC_READ_TABLE limit of characters per row, colums are splited into smaller lists
        lists_of_columns = []
        cols = []
        col_length_total = 0
        for col in columns:
            info = self.call("DDIF_FIELDINFO_GET", TABNAME=table_name, FIELDNAME=col)
            col_length = info["DFIES_TAB"][0]["LENG"]
            col_length_total += int(col_length)
            if col_length_total <= character_limit:
                cols.append(col)
            else:
                lists_of_columns.append(cols)
                cols = [col]
                col_length_total = 0
        lists_of_columns.append(cols)

        columns = lists_of_columns
        options = [{"TEXT": where}] if where else None
        limit = self._get_limit(sql)
        offset = self._get_offset(sql)
        query_json = dict(
            QUERY_TABLE=table_name,
            FIELDS=columns,
            OPTIONS=options,
            ROWCOUNT=limit,
            ROWSKIPS=offset,
            DELIMITER=sep,
        )
        # SAP doesn't understand None, so we filter out non-specified parameters
        query_json_filtered = {
            key: query_json[key] for key in query_json if query_json[key] is not None
        }
        self._query = query_json_filtered

    def call(self, func: str, *args, **kwargs):
        """Call a SAP RFC function"""
        return self.con.call(func, *args, **kwargs)

    def _get_alias(self, column: str) -> str:
        return self.aliases_keyed_by_columns.get(column, column)

    def _get_client_side_filter_cols(self):
        return [f[1].split()[0] for f in self.client_side_filters.items()]

    def to_df(self):
        """
        Load the results of a query into a pandas DataFrame.

        Due to SAP limitations, if the length of the WHERE clause is longer than 75
        characters, we trim whe WHERE clause and perform the rest of the filtering
        on the resulting DataFrame. Eg. if the WHERE clause contains 4 conditions
        and has 80 characters, we only perform 3 filters in the query, and perform
        the last filter on the DataFrame. If characters per row limit will be exceeded,
        data will be downloaded in chunks.

        Source: https://success.jitterbit.com/display/DOC/Guide+to+Using+RFC_READ_TABLE+to+Query+SAP+Tables#GuidetoUsingRFC_READ_TABLEtoQuerySAPTables-create-the-operation
        - WHERE clause: 75 character limit
        - SELECT: 512 character row limit

        Returns:
            pd.DataFrame: A DataFrame representing the result of the query provided in `PyRFC.query()`.
        """
        params = self._query
        columns = self.select_columns_aliased
        sep = self._query.get("DELIMITER")
        fields_lists = self._query.get("FIELDS")
        if len(fields_lists) > 1:
            logger.info(f"Data will be downloaded in {len(fields_lists)} chunks.")
        func = self.func
        if sep is None:
            # automatically find a working separator
            SEPARATORS = [
                "|",
                "/t",
                "#",
                ";",
                "@",
                "%",
                "^",
                "`",
                "~",
                "{",
                "}",
                "$",
            ]
        else:
            SEPARATORS = [sep]

        records = None
        for sep in SEPARATORS:
            logger.info(f"Checking if separator '{sep}' works.")
            df = pd.DataFrame()
            self._query["DELIMITER"] = sep
            chunk = 1
            for fields in fields_lists:
                logger.info(f"Downloading {chunk} data chunk...")
                try:
                    self._query["FIELDS"] = fields
                    try:
                        response = self.call(func, **params)
                    except ABAPApplicationError as e:
                        if e.key == "DATA_BUFFER_EXCEEDED":
                            raise DataBufferExceeded(
                                "Character limit per row exceeded. Please select fewer columns."
                            )
                        else:
                            raise e
                    record_key = "WA"
                    data_raw = response["DATA"]
                    records = [row[record_key].split(sep) for row in data_raw]
                    df[fields] = records
                    chunk += 1
                except ValueError:
                    df = pd.DataFrame()
                    continue
        if not records:
            logger.warning("Empty output was generated.")
            columns = []
        df.columns = columns

        if self.client_side_filters:
            filter_query = self._build_pandas_filter_query(self.client_side_filters)
            df.query(filter_query, inplace=True)
            client_side_filter_cols_aliased = [
                self._get_alias(col) for col in self._get_client_side_filter_cols()
            ]
            cols_to_drop = [
                col
                for col in client_side_filter_cols_aliased
                if col not in self.select_columns_aliased
            ]
            df.drop(cols_to_drop, axis=1, inplace=True)
        self.close_connection()
        return df


class SAPRFCV2(Source):
    """
    A class for querying SAP with SQL using the RFC protocol.

    Note that only a very limited subset of SQL is supported:
    - aliases
    - where clauses combined using the AND operator
    - limit & offset

    Unsupported:
    - aggregations
    - joins
    - subqueries
    - etc.
    """

    def __init__(
        self,
        sep: str = None,
        replacement: str = "-",
        func: str = "RFC_READ_TABLE",
        rfc_total_col_width_character_limit: int = 400,
        rfc_unique_id: List[str] = None,
        *args,
        **kwargs,
    ):
        """Create an instance of the SAPRFC class.

        Args:
            sep (str, optional): Which separator to use when querying SAP. If not provided,
            multiple options are automatically tried.
            replacement (str, optional): In case of separator is on a columns, set up a new character to replace
                inside the string to avoid flow breakdowns. Defaults to "-".
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            rfc_total_col_width_character_limit (int, optional): Number of characters by which query will be split in chunks
            in case of too many columns for RFC function. According to SAP documentation, the limit is
            512 characters. However, we observed SAP raising an exception even on a slightly lower number
            of characters, so we add a safety margin. Defaults to 400.
            rfc_unique_id  (List[str], optional): Reference columns to merge chunks Data Frames. These columns must to be unique. Defaults to None.

        Raises:
            CredentialError: If provided credentials are incorrect.
        """

        self._con = None
        DEFAULT_CREDENTIALS = local_config.get("SAP").get("DEV")
        credentials = kwargs.pop("credentials", None) or DEFAULT_CREDENTIALS
        if credentials is None:
            raise CredentialError("Missing credentials.")

        super().__init__(*args, credentials=credentials, **kwargs)

        self.sep = sep
        self.replacement = replacement
        self.client_side_filters = None
        self.func = func
        self.rfc_total_col_width_character_limit = rfc_total_col_width_character_limit
        # remove repeated reference columns
        if rfc_unique_id is not None:
            self.rfc_unique_id = list(set(rfc_unique_id))
        else:
            self.rfc_unique_id = rfc_unique_id

    @property
    def con(self) -> pyrfc.Connection:
        if self._con is not None:
            return self._con
        con = pyrfc.Connection(**self.credentials)
        self._con = con
        return con

    def check_connection(self) -> None:
        self.logger.info("Checking the connection...")
        self.con.ping()
        self.logger.info("Connection has been validated successfully.")

    def close_connection(self) -> None:
        """Closing RFC connection."""
        self.con.close()
        self.logger.info("Connection has been closed successfully.")

    def get_function_parameters(
        self,
        function_name: str,
        description: Union[None, Literal["short", "long"]] = "short",
        *args,
    ) -> Union[List[str], pd.DataFrame]:
        """Get the description for a SAP RFC function.

        Args:
            function_name (str): The name of the function to detail.
            description (Union[None, Literal[, optional): Whether to display
            a short or a long description. Defaults to "short".

        Raises:
            ValueError: If the argument for description is incorrect.

        Returns:
            Union[List[str], pd.DataFrame]: Either a list of the function's
            parameter names (if 'description' is set to None),
            or a short or long description.
        """
        if description is not None:
            if description not in ["short", "long"]:
                raise ValueError(
                    "Incorrect value for 'description'. Correct values: (None, 'short', 'long'"
                )

        descr = self.con.get_function_description(function_name, *args)
        param_names = [param["name"] for param in descr.parameters]
        detailed_params = descr.parameters
        filtered_detailed_params = [
            {
                "name": param["name"],
                "parameter_type": param["parameter_type"],
                "default_value": param["default_value"],
                "optional": param["optional"],
                "parameter_text": param["parameter_text"],
            }
            for param in descr.parameters
        ]

        if description is not None:
            if description == "long":
                params = detailed_params
            else:
                params = filtered_detailed_params
            params = pd.DataFrame.from_records(params)
        else:
            params = param_names

        return params

    def _get_where_condition(self, sql: str) -> str:
        """Retrieve the WHERE conditions from a SQL query.

        Args:
            sql (str): The input SQL query.

        Raises:
            ValueError: Raised if the WHERE clause is longer than
            75 characters (SAP's limitation) and the condition for the
            extra clause(s) is OR.

        Returns:
            str: The where clause trimmed to <= 75 characters.
        """

        where_match = re.search("\\sWHERE ", sql.upper())
        if not where_match:
            return None

        limit_match = re.search("\\sLIMIT ", sql.upper())
        limit_pos = limit_match.span()[0] if limit_match else len(sql)

        where = sql[where_match.span()[1] : limit_pos]
        where_sanitized = remove_whitespaces(where)
        where_trimmed, client_side_filters = trim_where(where_sanitized)
        if client_side_filters:
            self.logger.warning(
                "A WHERE clause longer than 75 character limit detected."
            )
            if "OR" in [key.upper() for key in client_side_filters.keys()]:
                raise ValueError(
                    "WHERE conditions after the 75 character limit can only be combined with the AND keyword."
                )
            else:
                filters_pretty = list(client_side_filters.items())
                self.logger.warning(
                    f"Trimmed conditions ({filters_pretty}) will be applied client-side."
                )
                self.logger.warning(f"See the documentation for caveats.")

        self.client_side_filters = client_side_filters
        return where_trimmed

    @staticmethod
    def _get_table_name(sql: str) -> str:
        parsed = Parser(sql)
        if len(parsed.tables) > 1:
            raise ValueError("Querying more than one table is not supported.")
        return parsed.tables[0]

    def _build_pandas_filter_query(
        self, client_side_filters: OrderedDictType[str, str]
    ) -> str:
        """Build a WHERE clause that will be applied client-side.
        This is required if the WHERE clause passed to query() is
        longer than 75 characters.

        Args:
            client_side_filters (OrderedDictType[str, str]): The
            client-side filters to apply.

        Returns:
            str: the WHERE clause reformatted to fit the format
            required by DataFrame.query().
        """
        for i, f in enumerate(client_side_filters.items()):
            if i == 0:
                # skip the first keyword; we assume it's "AND"
                query = f[1]
            else:
                query += " " + f[0] + " " + f[1]

            filter_column_name = f[1].split()[0]
            resolved_column_name = self._resolve_col_name(filter_column_name)
        query = re.sub("\\s?=\\s?", " == ", query).replace(
            filter_column_name, resolved_column_name
        )
        return query

    def extract_values(self, sql: str) -> None:
        """TODO: This should cover all values, not just columns"""
        self.where = self._get_where_condition(sql)
        self.select_columns = self._get_columns(sql, aliased=False)
        self.select_columns_aliased = self._get_columns(sql, aliased=True)

    def _resolve_col_name(self, column: str) -> str:
        """Get aliased column name if it exists, otherwise return column name."""
        return self.aliases_keyed_by_columns.get(column, column)

    def _get_columns(self, sql: str, aliased: bool = False) -> List[str]:
        """Retrieve column names from a SQL query.

        Args:
            sql (str): The SQL query to parse.
            aliased (bool, optional): Whether to returned aliased
            names. Defaults to False.

        Returns:
            List[str]: A list of column names.
        """
        parsed = Parser(sql)
        columns = list(parsed.columns_dict["select"])
        if aliased:
            aliases_keyed_by_alias = parsed.columns_aliases
            aliases_keyed_by_columns = OrderedDict(
                {val: key for key, val in aliases_keyed_by_alias.items()}
            )

            self.aliases_keyed_by_columns = aliases_keyed_by_columns

            columns = [
                aliases_keyed_by_columns[col]
                if col in aliases_keyed_by_columns
                else col
                for col in columns
            ]

        if self.client_side_filters:
            # In case the WHERE clause is > 75 characters long, we execute the rest of the filters
            # client-side. To do this, we need to pull all fields in the client-side WHERE conditions.
            # Below code adds these columns to the list of SELECTed fields.
            cols_to_add = [v.split()[0] for v in self.client_side_filters.values()]
            if aliased:
                cols_to_add = [aliases_keyed_by_columns[col] for col in cols_to_add]
            columns.extend(cols_to_add)
            columns = list(dict.fromkeys(columns))  # remove duplicates

        return columns

    @staticmethod
    def _get_limit(sql: str) -> int:
        """Get limit from the query"""
        limit_match = re.search("\\sLIMIT ", sql.upper())
        if not limit_match:
            return None

        return int(sql[limit_match.span()[1] :].split()[0])

    @staticmethod
    def _get_offset(sql: str) -> int:
        """Get offset from the query"""
        offset_match = re.search("\\sOFFSET ", sql.upper())
        if not offset_match:
            return None

        return int(sql[offset_match.span()[1] :].split()[0])

    def query(self, sql: str, sep: str = None) -> None:
        """Parse an SQL query into pyRFC commands and save it into
        an internal dictionary.

        Args:
            sql (str): The SQL query to be ran.
            sep (str, optional): The separator to be used
            to split columns in the result blob. Defaults to self.sep.

        Raises:
            ValueError: If the query is not a SELECT query.
        """

        if not sql.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT queries are supported.")

        sep = sep if sep is not None else self.sep

        self.sql = sql

        self.extract_values(sql)

        table_name = self._get_table_name(sql)
        # this has to be called before checking client_side_filters
        where = self.where
        columns = self.select_columns
        lists_of_columns = []
        cols = []
        col_length_total = 0
        if isinstance(self.rfc_unique_id[0], str):
            character_limit = self.rfc_total_col_width_character_limit
            for ref_column in self.rfc_unique_id:
                col_length_reference_column = int(
                    self.call(
                        "DDIF_FIELDINFO_GET",
                        TABNAME=table_name,
                        FIELDNAME=ref_column,
                    )["DFIES_TAB"][0]["LENG"]
                )
                if col_length_reference_column > int(
                    self.rfc_total_col_width_character_limit / 4
                ):
                    raise ValueError(
                        f"{ref_column} can't be used as unique column, too large."
                    )
                local_limit = (
                    self.rfc_total_col_width_character_limit
                    - col_length_reference_column
                )
                if local_limit < character_limit:
                    character_limit = local_limit
        else:
            character_limit = self.rfc_total_col_width_character_limit

        for col in columns:
            info = self.call("DDIF_FIELDINFO_GET", TABNAME=table_name, FIELDNAME=col)
            col_length = info["DFIES_TAB"][0]["LENG"]
            col_length_total += int(col_length)
            if col_length_total <= character_limit:
                cols.append(col)
            else:
                if isinstance(self.rfc_unique_id[0], str) and all(
                    [rfc_col not in cols for rfc_col in self.rfc_unique_id]
                ):
                    for rfc_col in self.rfc_unique_id:
                        if rfc_col not in cols:
                            cols.append(rfc_col)
                lists_of_columns.append(cols)
                cols = [col]
                col_length_total = int(col_length)
        else:
            if isinstance(self.rfc_unique_id[0], str) and all(
                [rfc_col not in cols for rfc_col in self.rfc_unique_id]
            ):
                for rfc_col in self.rfc_unique_id:
                    if rfc_col not in cols:
                        cols.append(rfc_col)
            lists_of_columns.append(cols)

        columns = lists_of_columns
        options = [{"TEXT": where}] if where else None
        limit = self._get_limit(sql)
        offset = self._get_offset(sql)
        query_json = dict(
            QUERY_TABLE=table_name,
            FIELDS=columns,
            OPTIONS=options,
            ROWCOUNT=limit,
            ROWSKIPS=offset,
            DELIMITER=sep,
        )
        # SAP doesn't understand None, so we filter out non-specified parameters
        query_json_filtered = {
            key: query_json[key] for key in query_json if query_json[key] is not None
        }
        self._query = query_json_filtered

    def call(self, func: str, *args, **kwargs):
        """Call a SAP RFC function"""
        return self.con.call(func, *args, **kwargs)

    def _get_alias(self, column: str) -> str:
        return self.aliases_keyed_by_columns.get(column, column)

    def _get_client_side_filter_cols(self):
        return [f[1].split()[0] for f in self.client_side_filters.items()]

    def to_df(self):
        """
        Load the results of a query into a pandas DataFrame.

        Due to SAP limitations, if the length of the WHERE clause is longer than 75
        characters, we trim whe WHERE clause and perform the rest of the filtering
        on the resulting DataFrame. Eg. if the WHERE clause contains 4 conditions
        and has 80 characters, we only perform 3 filters in the query, and perform
        the last filter on the DataFrame. If characters per row limit will be exceeded,
        data will be downloaded in chunks.

        Source: https://success.jitterbit.com/display/DOC/Guide+to+Using+RFC_READ_TABLE+to+Query+SAP+Tables#GuidetoUsingRFC_READ_TABLEtoQuerySAPTables-create-the-operation
        - WHERE clause: 75 character limit
        - SELECT: 512 character row limit

        Returns:
            pd.DataFrame: A DataFrame representing the result of the query provided in `PyRFC.query()`.
        """
        params = self._query
        columns = self.select_columns_aliased
        sep = self._query.get("DELIMITER")
        fields_lists = self._query.get("FIELDS")
        if len(fields_lists) > 1:
            logger.info(f"Data will be downloaded in {len(fields_lists)} chunks.")
        func = self.func
        if sep is None:
            # automatically find a working separator
            SEPARATORS = [
                "|",
                "/t",
                "#",
                ";",
                "@",
                "%",
                "^",
                "`",
                "~",
                "{",
                "}",
                "$",
            ]
        else:
            SEPARATORS = [sep]

        for sep in SEPARATORS:
            logger.info(f"Checking if separator '{sep}' works.")
            if isinstance(self.rfc_unique_id[0], str):
                # columns only for the first chunk and we add the rest later to avoid name conflicts
                df = pd.DataFrame(columns=fields_lists[0])
            else:
                df = pd.DataFrame()
            self._query["DELIMITER"] = sep
            chunk = 1
            row_index = 0
            for fields in fields_lists:
                logger.info(f"Downloading {chunk} data chunk...")
                self._query["FIELDS"] = fields
                try:
                    response = self.call(func, **params)
                except ABAPApplicationError as e:
                    if e.key == "DATA_BUFFER_EXCEEDED":
                        raise DataBufferExceeded(
                            "Character limit per row exceeded. Please select fewer columns."
                        )
                    else:
                        raise e
                record_key = "WA"
                data_raw = np.array(response["DATA"])

                # if the reference columns are provided not necessary to remove any extra row.
                if not isinstance(self.rfc_unique_id[0], str):
                    row_index, data_raw, start = detect_extra_rows(
                        row_index, data_raw, chunk, fields
                    )
                else:
                    start = False

                data_raw = catch_extra_separators(
                    data_raw, record_key, sep, fields, self.replacement
                )

                records = np.array([row[record_key].split(sep) for row in data_raw])

                if (
                    isinstance(self.rfc_unique_id[0], str)
                    and not list(df.columns) == fields
                ):
                    df_tmp = pd.DataFrame(columns=fields)
                    df_tmp[fields] = records
                    df = pd.merge(df, df_tmp, on=self.rfc_unique_id, how="outer")
                else:
                    if not start:
                        df[fields] = records
                    else:
                        df[fields] = np.nan
                chunk += 1
        df.columns = columns

        if self.client_side_filters:
            filter_query = self._build_pandas_filter_query(self.client_side_filters)
            df.query(filter_query, inplace=True)
            client_side_filter_cols_aliased = [
                self._get_alias(col) for col in self._get_client_side_filter_cols()
            ]
            cols_to_drop = [
                col
                for col in client_side_filter_cols_aliased
                if col not in self.select_columns_aliased
            ]
            df.drop(cols_to_drop, axis=1, inplace=True)
        self.close_connection()
        return df
