"""SAP RFC connectors."""

from collections import OrderedDict
from collections import OrderedDict as OrderedDictType
from collections.abc import Iterable, Iterator
import logging
import re
from typing import (
    Any,
    Literal,
)

import numpy as np
from numpy.typing import ArrayLike
import pandas as pd
import pyrfc


try:
    import pyrfc
    from pyrfc._exception import ABAPApplicationError
except ModuleNotFoundError as e:
    msg = "Missing required modules to use SAPRFC source."
    raise ImportError(msg) from e

from sql_metadata import Parser

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError, DataBufferExceededError
from viadot.orchestration.prefect.utils import DynamicDateHandler
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns, validate


logger = logging.getLogger()


def adjust_where_condition_by_adding_missing_spaces(sql: str) -> str:
    """Add white spaces between operators and `WHERE` statement.

    This function is taking raw sql string and sanitizing it at the beginning of the
    'query()' method, so other methods that taking sql as parameter could have sql
    without whitespaces issues.

    Args:
        sql (str): raw sql query passed in flow

    Returns:
        str: sql query after adding white spaces if needed
    """
    # Check if 'WHERE' statement is not attached to 'FROM' or column name as there is
    # a need for space on both sides of 'WHERE'.
    sql = re.sub(rf"{re.escape('WHERE')}(?<!\s)", "WHERE ", sql, flags=re.IGNORECASE)
    sql = re.sub(rf"(?<!\s){re.escape('WHERE')}", " WHERE", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s+", " ", sql)

    # Check if operators are not attached to column or value as there is need for space
    # on both sides of operator.
    operators = ["<>", "!=", "<=", ">=", "!<", "!>", "=", ">", "<"]
    reverse_check = [
        "< >",
        "! =",
        "< =",
        "> =",
        "! <",
        "! >",
    ]

    for op in operators:
        sql = re.sub(rf"(?<!\s){re.escape(op)}", f" {op}", sql)
        sql = re.sub(rf"{re.escape(op)}(?<!\s)", f"{op} ", sql)
        sql = re.sub(r"\s+", " ", sql)
    for op_2 in reverse_check:
        if op_2 in sql:
            sql = sql.replace(op_2, "".join(op_2.split()))

    return sql


def _remove_whitespaces(text: str) -> str:
    return " ".join(text.split())


def _get_keyword_for_condition(where: str, condition: str) -> str:
    where = where[: where.find(condition)]
    return where.split()[-1]


def _get_where_uppercased(where: str) -> str:
    """Uppercase a WHERE clause's keywords without altering the original string."""
    where_and_uppercased = re.sub("\\sand ", " AND ", where)
    return re.sub("\\sor ", " OR ", where_and_uppercased)


def _remove_last_condition(where: str) -> str:
    """Remove the last condtion from a WHERE clause."""
    where = _get_where_uppercased(where)
    split_by_and = re.split("\\sAND ", where)
    conditions = [re.split("\\sOR ", expr) for expr in split_by_and]
    conditions_flattened = [
        condition for sublist in conditions for condition in sublist
    ]

    condition_to_remove = conditions_flattened[-1]

    where_trimmed = where[: where.find(condition_to_remove)].split()
    where_trimmed_without_last_keyword = " ".join(where_trimmed[:-1])

    return where_trimmed_without_last_keyword, condition_to_remove


def _trim_where(where: str) -> tuple[str, OrderedDictType[str, str] | None]:
    """Trim a WHERE clause to 75 characters or less, as required by SAP RFC.

    The rest of filters will be applied in-memory on client side.
    """
    if len(where) <= SAPRFC.COL_CHARACTER_WIDTH_LIMIT:
        return where, None

    wheres_to_add = OrderedDict()
    keywords_with_conditions = []
    where_trimmed = where
    while len(where_trimmed) > SAPRFC.COL_CHARACTER_WIDTH_LIMIT:
        # trim the where
        where_trimmed, removed_condition = _remove_last_condition(where_trimmed)

        # store the removed conditions so we can readd them later
        keyword = _get_keyword_for_condition(where, removed_condition)
        keywords_with_conditions.append((keyword, removed_condition))

    wheres_to_add_sorted = keywords_with_conditions[::-1]
    wheres_to_add = OrderedDict(wheres_to_add_sorted)

    return where_trimmed, wheres_to_add


def _detect_extra_rows(
    row_index: int,
    data_raw: ArrayLike,
    chunk: int,
    fields: list[str],
) -> int | ArrayLike | bool:
    """Check if, in between calls to the SAP table, the number of rows has increased.

    If so, remove the last rows added, to fit the size of the previous columns.

    Args:
        row_index (int): Number of rows set it down in he first iteration with the SAP
            table.
        data_raw (ArrayLike): Array with the data retrieve from SAP table.
        chunk (int): The part number in which a number of SAP table columns have been
            split.
        fields (List[str]): A list with the names of the columns in a chunk.

    Returns:
        Union[int, ArrayLike, bool]: A tuple with the parameters "row_index",
            "data_raw", a new boolean variable "start" to indicate when the for loop has
            to be restarted, and "chunk" variable.
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
            "New rows were generated during the execution of the script. The table is truncated to the number of rows for the first chunk."
        )

    return row_index, data_raw, start


def _gen_split(data: Iterable[str], sep: str, record_key: str) -> Iterator[list[str]]:
    """Split each string in the given iterable using the specified separator.

    Helps to reduce memory usage when processing big data sets by yielding the resulting
    list.

    Args:
        data: An iterable collection of strings to be split.
        sep: The string separator used to split each string in `data`.
        record_key: A key for extraction of the data from the nested dict.

    Yields:
        A list of substrings for each string in `data`, split using `sep`.

    Examples:
        >>> list(gen_strip(["a|b|c", "d|e|f|g"], "|", "WA"))
        [['a', 'b', 'c'], ['d', 'e', 'f', 'g']]
    """
    for row in data:
        yield row[record_key].split(sep)


class SAPRFC(Source):
    """A class for querying SAP with SQL using the RFC protocol.

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

    COL_CHARACTER_WIDTH_LIMIT = 75

    def __init__(
        self,
        sep: str | None = "♔",
        replacement: str = "-",
        func: str = "RFC_READ_TABLE",
        rfc_total_col_width_character_limit: int = 400,
        rfc_unique_id: list[str] | None = None,
        credentials: dict[str, Any] | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Create an instance of the SAPRFC class.

        Args:
            sep (str, optional): The separator to use when reading query results.
                If set to None, multiple options are automatically tried.
                Defaults to ♔.
            replacement (str, optional): In case of separator is on a columns, set up a
                new character to replace inside the string to avoid flow breakdowns.
                Defaults to "-".
            func (str, optional): SAP RFC function to use. Defaults to "RFC_READ_TABLE".
            rfc_total_col_width_character_limit (int, optional): Number of characters by
                which query will be split in chunks in case of too many columns for RFC
                function. According to SAP documentation, the limit is 512 characters.
                However, we observed SAP raising an exception even on a slightly lower
                number of characters, so we add a safety margin. Defaults to 400.
            rfc_unique_id  (List[str], optional): Reference columns to merge chunks
                DataFrames. These columns must to be unique. Defaults to None.
            credentials (Dict[str, Any], optional): 'api_key'. Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant
                credentials.

        Raises:
            CredentialError: If provided credentials are incorrect.
        """
        self._con = None

        credentials = credentials or get_source_credentials(config_key)
        if credentials is None:
            msg = "Please specify the credentials."
            raise CredentialError(msg)

        super().__init__(*args, credentials=credentials, **kwargs)

        self.sep = sep
        self.replacement = replacement
        self.client_side_filters = None
        self.func = func
        self.rfc_total_col_width_character_limit = rfc_total_col_width_character_limit

        if rfc_unique_id is not None:
            self.rfc_unique_id = list(set(rfc_unique_id))
            self._rfc_unique_id_len = {}
        else:
            self.rfc_unique_id = None

    @property
    def con(self) -> pyrfc.Connection:
        """The pyRFC connection to SAP."""
        if self._con is not None:
            return self._con
        con = pyrfc.Connection(**self.credentials)
        self._con = con
        return con

    def check_connection(self) -> None:
        """Check the connection to SAP."""
        self.logger.info("Checking the connection...")
        self.con.ping()
        self.logger.info("Connection has been validated successfully.")

    def close_connection(self) -> None:
        """Close the SAP RFC connection."""
        self.con.close()
        self.logger.info("Connection has been closed successfully.")

    def get_function_parameters(
        self,
        function_name: str,
        description: None | Literal["short", "long"] = "short",
        *args,
    ) -> list[str] | pd.DataFrame:
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
        if description not in ["short", "long"]:
            msg = "Incorrect value for 'description'. Correct values: None, 'short', 'long'."
            raise ValueError(msg)

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
        where_sanitized = _remove_whitespaces(where)
        where_trimmed, client_side_filters = _trim_where(where_sanitized)
        if client_side_filters:
            self.logger.warning(
                "A WHERE clause longer than 75 character limit detected."
            )
            if "OR" in [key.upper() for key in client_side_filters]:
                msg = "WHERE conditions after the 75 character limit can only be combined with the AND keyword."
                raise ValueError(msg)
            for val in client_side_filters.values():
                if ")" in val:
                    msg = "Nested conditions eg. AND (col_1 = 'a' AND col_2 = 'b') found between or after 75 characters in WHERE condition!"
                    msg += " Please change nested conditions part of query separated with 'AND' keywords,"
                    msg += " or place nested conditions part at the beginning of the where statement."
                    raise ValueError(msg)
            filters_pretty = list(client_side_filters.items())
            self.logger.warning(
                f"Trimmed conditions ({filters_pretty}) will be applied client-side."
            )
            self.logger.warning("See the documentation for caveats.")

        self.client_side_filters = client_side_filters
        return where_trimmed

    @staticmethod
    def _get_table_name(sql: str) -> str:
        parsed = Parser(sql)
        if len(parsed.tables) > 1:
            msg = "Querying more than one table is not supported."
            raise ValueError(msg)
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
        return re.sub("\\s?=\\s?", " == ", query).replace(
            filter_column_name, resolved_column_name
        )

    def extract_values(self, sql: str) -> None:
        """TODO: This should cover all values, not just columns."""
        self.where = self._get_where_condition(sql)
        self.select_columns = self._get_columns(sql, aliased=False)
        self.select_columns_aliased = self._get_columns(sql, aliased=True)

    def _resolve_col_name(self, column: str) -> str:
        """Get aliased column name if it exists, otherwise return column name."""
        return self.aliases_keyed_by_columns.get(column, column)

    def _get_columns(self, sql: str, aliased: bool = False) -> list[str]:
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

            columns = [aliases_keyed_by_columns.get(col, col) for col in columns]

        if self.client_side_filters:
            # In case the WHERE clause is > 75 characters long, we execute the rest of
            # the filters client-side. To do this, we need to pull all fields in the
            # client-side WHERE conditions. Below code adds these columns to the list of
            # SELECTed fields.
            cols_to_add = [v.split()[0] for v in self.client_side_filters.values()]
            if aliased:
                cols_to_add = [aliases_keyed_by_columns[col] for col in cols_to_add]
            columns.extend(cols_to_add)
            columns = list(dict.fromkeys(columns))  # remove duplicates

        return columns

    @staticmethod
    def _get_limit(sql: str) -> int | None:
        """Get limit from the query."""
        limit_match = re.search("\\sLIMIT ", sql.upper())
        if not limit_match:
            return None

        return int(sql[limit_match.span()[1] :].split()[0])

    @staticmethod
    def _get_offset(sql: str) -> int | None:
        """Get offset from the query."""
        offset_match = re.search("\\sOFFSET ", sql.upper())
        if not offset_match:
            return None

        return int(sql[offset_match.span()[1] :].split()[0])

    def _parse_dates(
        self,
        query: str,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y%m%d",
        dynamic_date_timezone: str = "UTC",
    ) -> str:
        """Process dynamic dates inside the query and validate used patterns type.

        Args:
            query (str): The SQL query to be processed.
            dynamic_date_symbols (list[str], optional): Symbols used for dynamic date
                handling. Defaults to ["<<", ">>"].
            dynamic_date_format (str, optional): Format used for dynamic date parsing.
                Defaults to "%Y%m%d".
            dynamic_date_timezone (str, optional): Timezone used for dynamic date
                processing. Defaults to "UTC".

        Returns:
            str: The processed SQL query with dynamic dates replaced.

        Raises:
            TypeError: If the query contains dynamic date patterns that generate
        a range of dates.
        """
        ddh = DynamicDateHandler(
            dynamic_date_symbols=dynamic_date_symbols,
            dynamic_date_format=dynamic_date_format,
            dynamic_date_timezone=dynamic_date_timezone,
        )
        processed_sql_or_list = ddh.process_dates(query)

        if isinstance(processed_sql_or_list, list):
            msg = (
                f"This query contains {ddh._find_dynamic_date_patterns(query)} dynamic date(s) "
                "that generate a range of dates, which is currently not supported"
                "in query generation. Please use one of the singular pattern dynamic date symbols."
            )
            raise TypeError(msg)

        return processed_sql_or_list

    # Holy crap what a mess. TODO: refactor this so it can be even remotely tested...
    def query(self, sql: str, sep: str | None = None) -> None:  # noqa: C901, PLR0912, PLR0915
        """Parse an SQL query into pyRFC commands and save it into an internal dict.

        Args:
            sql (str): The SQL query to be ran.
            sep (str, optional): The separator to be used
            to split columns in the result blob. Defaults to self.sep.

        Raises:
            ValueError: If the query is not a SELECT query.
        """
        if not sql.strip().upper().startswith("SELECT"):
            msg = "Only SELECT queries are supported."
            raise ValueError(msg)

        sep = sep if sep is not None else self.sep

        sql = adjust_where_condition_by_adding_missing_spaces(sql=sql)
        self.sql = sql

        self.extract_values(sql)

        table_name = self._get_table_name(sql)
        # this has to be called before checking client_side_filters
        where = self.where
        columns = self.select_columns
        lists_of_columns = []
        cols = []
        col_length_total = 0
        if isinstance(self.rfc_unique_id, list):
            character_limit = self.rfc_total_col_width_character_limit
            for rfc_unique_col in self.rfc_unique_id:
                rfc_unique_col_len = int(
                    self.call(
                        "DDIF_FIELDINFO_GET",
                        TABNAME=table_name,
                        FIELDNAME=rfc_unique_col,
                    )["DFIES_TAB"][0]["LENG"]
                )
                if rfc_unique_col_len > int(
                    self.rfc_total_col_width_character_limit / 4
                ):
                    msg = f"{rfc_unique_col} can't be used as unique column, too large."
                    raise ValueError(msg)
                local_limit = (
                    self.rfc_total_col_width_character_limit - rfc_unique_col_len
                )
                character_limit = min(local_limit, character_limit)
                self._rfc_unique_id_len[rfc_unique_col] = rfc_unique_col_len
        else:
            character_limit = self.rfc_total_col_width_character_limit

        for col in columns:
            info = self.call("DDIF_FIELDINFO_GET", TABNAME=table_name, FIELDNAME=col)
            col_length = info["DFIES_TAB"][0]["LENG"]
            col_length_total += int(col_length)
            if col_length_total <= character_limit:
                cols.append(col)
            else:
                if isinstance(self.rfc_unique_id, list):
                    if all(
                        rfc_unique_col not in cols
                        for rfc_unique_col in self.rfc_unique_id
                    ):
                        for rfc_unique_col in self.rfc_unique_id:
                            if rfc_unique_col not in cols:
                                cols.append(rfc_unique_col)
                else:
                    msg = "Columns character limit per row exceeded. Please select fewer columns or add rfc_unique_id."
                    raise DataBufferExceededError(msg)
                lists_of_columns.append(cols)
                cols = [col]
                col_length_total = int(col_length)

        if isinstance(self.rfc_unique_id, list) and all(
            rfc_unique_col not in cols for rfc_col in self.rfc_unique_id
        ):
            for rfc_unique_col in self.rfc_unique_id:
                if rfc_unique_col not in cols:
                    cols.append(rfc_unique_col)
        lists_of_columns.append(cols)

        columns = lists_of_columns
        options = [{"TEXT": where}] if where else None
        limit = self._get_limit(sql)
        offset = self._get_offset(sql)
        query_json = {
            "QUERY_TABLE": table_name,
            "FIELDS": columns,
            "OPTIONS": options,
            "ROWCOUNT": limit,
            "ROWSKIPS": offset,
            "DELIMITER": sep,
        }
        # SAP doesn't understand None, so we filter out non-specified parameters
        query_json_filtered = {
            key: query_json[key] for key in query_json if query_json[key] is not None
        }
        self._query = query_json_filtered

    def call(self, func: str, *args, **kwargs) -> dict[str, Any]:
        """Call a SAP RFC function."""
        return self.con.call(func, *args, **kwargs)

    def _get_alias(self, column: str) -> str:
        return self.aliases_keyed_by_columns.get(column, column)

    def _get_client_side_filter_cols(self):
        return [f[1].split()[0] for f in self.client_side_filters.items()]

    def _adjust_whitespaces(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adjust the number of whitespaces.

        Add whitespace characters in each row of each unique column to achieve
        equal length of values in these columns, ensuring proper merging of subqueries.

        """
        for rfc_unique_col in self.rfc_unique_id:
            # Check in SAP metadata what is the declared
            # dtype characters amount
            rfc_unique_column_len = self._rfc_unique_id_len[rfc_unique_col]
            actual_length_of_field = df[rfc_unique_col].str.len()
            # Check which rows have fewer characters
            # than specified in the column data type.
            rows_missing_whitespaces = actual_length_of_field < rfc_unique_column_len
            if any(rows_missing_whitespaces):
                # Check how many whitespaces are missing in each row.
                logger.info(f"Adding whitespaces for {rfc_unique_col} column")
                n_missing_whitespaces = rfc_unique_column_len - actual_length_of_field
                df.loc[rows_missing_whitespaces, rfc_unique_col] += np.char.multiply(
                    " ", n_missing_whitespaces[rows_missing_whitespaces]
                )
        return df

    # TODO: refactor to remove linter warnings and so this can be tested.
    @add_viadot_metadata_columns
    def to_df(self, tests: dict | None = None) -> pd.DataFrame:  # noqa: C901, PLR0912, PLR0915
        """Load the results of a query into a pandas DataFrame.

        Due to SAP limitations, if the length of the WHERE clause is longer than 75
        characters, we trim whe WHERE clause and perform the rest of the filtering
        on the resulting DataFrame. Eg. if the WHERE clause contains 4 conditions
        and has 80 characters, we only perform 3 filters in the query, and perform
        the last filter on the DataFrame. If characters per row limit will be exceeded,
        data will be downloaded in chunks.

        Source: https://success.jitterbit.com/display/DOC/Guide+to+Using+RFC_READ_TABLE+to+Query+SAP+Tables#GuidetoUsingRFC_READ_TABLEtoQuerySAPTables-create-the-operation
        - WHERE clause: 75 character limit
        - SELECT: 512 character row limit

        Args:
            tests (Dict[str], optional): A dictionary with optional list of tests
                to verify the output dataframe. If defined, triggers the `validate`
                function from utils. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame representing the result of the query provided in
                `PyRFC.query()`.
        """
        params = self._query
        sep = self._query.get("DELIMITER")
        fields_lists = self._query.get("FIELDS")
        if len(fields_lists) > 1:
            logger.info(f"Data will be downloaded in {len(fields_lists)} chunks.")
        func = self.func
        if sep is None:
            # Automatically find a working separator.
            separators = [
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
            separators = [sep]

        for sep in separators:
            logger.info(f"Checking if separator '{sep}' works.")
            if isinstance(self.rfc_unique_id, list):
                # Columns only for the first chunk. We add the rest later to avoid name
                # conflicts.
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
                        msg = "Character limit per row exceeded. Please select fewer columns."
                        raise DataBufferExceededError(msg) from e
                    raise
                # Check and skip if there is no data returned.
                if response["DATA"]:
                    record_key = "WA"
                    data_raw = np.array(response["DATA"])
                    del response
                    # If reference columns are provided, it's not necessary to remove
                    # any extra row.
                    if not isinstance(self.rfc_unique_id, list):
                        row_index, data_raw, start = _detect_extra_rows(
                            row_index, data_raw, chunk, fields
                        )
                    else:
                        start = False
                    records = list(_gen_split(data_raw, sep, record_key))
                    del data_raw
                    if (
                        isinstance(self.rfc_unique_id, list)
                        and list(df.columns) != fields
                    ):
                        df_tmp = pd.DataFrame(columns=fields)
                        df_tmp[fields] = records
                        df_tmp = self._adjust_whitespaces(df_tmp)
                        df = pd.merge(df, df_tmp, on=self.rfc_unique_id, how="outer")
                    elif not start:
                        df[fields] = records
                    else:
                        df[fields] = np.nan
                    chunk += 1
                elif not response["DATA"]:
                    logger.warning("No data returned from SAP.")
        if not df.empty:
            # It is used to filter out columns which are not in select query
            # for example columns passed only as unique column
            df = df.loc[:, self.select_columns]
            df.columns = self.select_columns_aliased

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

        if tests:
            validate(df=df, tests=tests)

        return df
