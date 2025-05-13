"""Common utilities for use in tasks."""

import contextlib
import json
from json.decoder import JSONDecodeError
import logging
import os
import sys
import tempfile
from typing import Any

import anyio
from anyio import open_process
from anyio.streams.text import TextReceiveStream
from prefect.blocks.system import Secret
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL


with contextlib.suppress(ModuleNotFoundError):
    from prefect_aws import AwsCredentials
    from prefect_aws.secrets_manager import AwsSecret
from prefect_sqlalchemy import DatabaseCredentials

from viadot.orchestration.prefect.exceptions import MissingPrefectBlockError


with contextlib.suppress(ModuleNotFoundError):
    from prefect_azure import AzureKeyVaultSecretReference

import re

import pendulum


class DynamicDateHandler:
    def __init__(
        self,
        dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
        dynamic_date_format: str = "%Y%m%d",
        dynamic_date_timezone: str = "Europe/Warsaw",
    ):
        """This class processes time-related patterns in the provided text.

        Replaces dynamic date markers with actual dates.
        The supported patterns include:
        - "today"
        - "yesterday"
        - "current_month"
        - "last_month"
        - "current_year"
        - "last_year"
        - "now_time"
        - "last_day_previous_month"
        - "last_day_of_month_year": e.g., "last_day_of_February_2020"
        - "X_years_ago_year": e.g., "3_years_ago_year" ,refers to only the year
            of the day X years ago
        - "X_years/months/days_ago_full_date":  e.g., "3_years_ago_full_date",
            refers to a given date X units ago in dynamic_date_format
        - "years_from_x_until_now_included": e.g. "years_from_2019_until_now_included",
           refers to a date range of the years from a given year
           until the current year included
        - "years_from_x_until_y_years_ago": e.g., "years_from_2019_until_y_years_ago",
          refers to a date range of the years from a given year X until y years ago
        - "last_X_years/months/days": e.g., "last_10_months", refers to a data range
            of the months in 'YMM' format
        - "first_X_days_from_X": e.g., "first_10_days_of_January_2020",
            returns a data range of days from a given month

        Args:
            dynamic_date_symbols (list[str], optional): The symbols that mark
                the start and the end of a dynamic date pattern in a text.
                Defaults to ["<<", ">>"].
            dynamic_date_format (str, optional): A date and time format string
                defining the text representation of date. Defaults to "%Y%m%d".
            dynamic_date_timezone (str, optional): A string that sets the default
                timezone used by all datetime functions. Defaults to "Europe/Warsaw".
        """
        self.singular_patterns = {
            "last_day_of_month": r"last_day_of_(\w+)_(\d{4})",
            "x_units_ago_full_date": r"(\d+)_(years?|months?|days?)_ago_full_date",
            "x_years_ago_year": r"(\d+)_years_ago_year",
        }
        self.range_patterns = {
            "last_x_units": r"last_(\d+)_(years|months|days)",
            "first_x_days_from": r"first_(\d+)_days_from_(\w+)_(\d{4})",
            "last_x_days_from": r"last_(\d+)_days_from_(\w+)_(\d{4})",
            "years_from_x_until_now": r"years_from_(\d{4})_until_now",
            "years_from_x_until_y_years_ago": r"years_from_(\d{4})_until_(\d+)_years_ago",
        }
        self.dynamic_date_format = dynamic_date_format
        self.dynamic_date_timezone = dynamic_date_timezone
        self.dynamic_date_symbols = dynamic_date_symbols
        self.replacements = self._create_date_dict()

    def _generate_years(
        self, last_years: int | None, from_year: str | None, end_year: str | None
    ) -> list[str]:
        """Generate a list of years either for the last X years or from a start year.

        Args:
            last_years (int | None): The number of years to generate
                from the current year.
            from_year (str | None): The starting year.
            end_year (int | None): The ending year.

        Returns:
            list: A list of years in ascending order.
        """
        current_year = pendulum.now().year
        if last_years:
            return [str(current_year - i) for i in range(last_years)][
                ::-1
            ]  # Reversed to ascending order
        if from_year and end_year:
            return [str(year) for year in range(int(from_year), int(end_year))]

        if from_year:
            return [str(year) for year in range(int(from_year), current_year + 1)]

        return []

    def _generate_months(self, last_months: int) -> list[str]:
        """Generate a list of first days of the last X months.

        Args:
            last_months (int): The number of months to include from the past.

        Returns:
            list: A list of dates representing the last X months in ascending order.
        """
        current_date = pendulum.now()

        return [
            current_date.subtract(months=i).start_of("month").format("YMM")
            for i in range(last_months)
        ][::-1]  # Reversed to ascending order

    def _generate_dates(self, last_days: int) -> list[str]:
        """Generate a list of dates for the last X days.

        Args:
            last_days (int): The number of days to include from the past.

        Returns:
            list: A list of dates in ascending order.
        """
        current_date = pendulum.now(self.dynamic_date_timezone)

        return [
            current_date.subtract(days=i).format("YMMDD") for i in range(last_days)
        ][::-1]  # Reversed to ascending order

    def _process_first_days(
        self, month_name: str, year: int, num_days: int
    ) -> list[str]:
        """Generate a list of the first X days of a given month and year.

        Args:
            month_name (str): The name of the month.
            year (str): The year.
            num_days (int): The number of days to include.

        Returns:
            list: A list of dates for the first X days in ascending order.
        """
        start_date = pendulum.datetime(
            int(year),
            pendulum.parse(month_name, strict=False).month,  # type: ignore
            1,  # type: ignore
        )

        return [
            start_date.add(days=i).format("YMMDD") for i in range(num_days)
        ]  # Ascending order

    def _process_last_days(
        self, month_name: str, year: int, num_days: int
    ) -> list[str]:
        """Generate a list of the last X days of a given month and year.

        Args:
            month_name (str): The name of the month.
            year (str): The year.
            num_days (int): The number of days to include.

        Returns:
            list: A list of dates for the last X days in ascending order.
        """
        start_date = pendulum.datetime(
            int(year),
            pendulum.parse(month_name, strict=False).month,  # type: ignore
            1,  # type: ignore
        )
        end_date = start_date.end_of("month")

        return [end_date.subtract(days=i).format("YMMDD") for i in range(num_days)][
            ::-1
        ]  # Reversed to ascending order

    def _process_last_day_of_month(
        self, year: str, month_name: str
    ) -> pendulum.DateTime:  # type: ignore
        """Retrieve the last day of a specified month and year.

        Args:
            year (str): The year.
            month_name (str): The name of the month.

        Returns:
            pendulum.DateTime: A date object containing the last day of the given month.
        """
        month_num = pendulum.parse(month_name, strict=False).month  # type: ignore

        return pendulum.datetime(int(year), month_num, 1).end_of("month")

    def _process_x_years_ago(self, year: int) -> str:
        """Retrieve the year of a date X years from now.

        Args:
            year (int): The year.

        Returns:
            str: A string containing the year of the specified time ago.
        """
        current_date = pendulum.now()

        return current_date.subtract(years=year).format("Y")

    def _get_date_x_ago_full_date(
        self, number: int, unit: str
    ) -> pendulum.DateTime | None:  # type: ignore
        """Retrieve the full date for X units ago from today.

        Args:
            number (int): The number of units (years, months, days).
            unit (str): The unit of time ('years', 'months', 'days').

        Returns:
            pendulum.DateTime: A date for X units ago from today.
        """
        return {
            "years": pendulum.now(self.dynamic_date_timezone).subtract(years=number),
            "months": pendulum.now(self.dynamic_date_timezone).subtract(months=number),
            "days": pendulum.now(self.dynamic_date_timezone).subtract(days=number),
        }.get(unit)

    def _create_date_dict(self) -> dict[str, str]:
        """Create and return a key phrase: dynamic date value dictionary.

        Dictionary values "today", "yesterday" and "last_year_previous_month" are
        formatted into the dynamic_date_format.

        The other values and their formatting:
            - "current_month" - A string date formatted with a string format '%m'.
            - "last_month" - A string date formatted with a format "%mm".
            - "current_year" - A string date formatted with a format "%mm".
            - "last_year" - A string date formatted with a string format '%Y'
            - "now_time" - A string date formatted with a string format '%H%M%S'.

        Returns:
            dict[str, str]: A dictionary with key phrases as keys
            and dynamically created dates as values.
        """
        today = pendulum.today(self.dynamic_date_timezone)
        yesterday = pendulum.yesterday(self.dynamic_date_timezone)
        last_month = today.subtract(months=1).month
        last_year = today.subtract(years=1)
        now_time = pendulum.now(self.dynamic_date_timezone)
        last_day_prev_month = today.subtract(months=1).end_of("month")

        return {
            "today": today.strftime(self.dynamic_date_format),
            "yesterday": yesterday.strftime(self.dynamic_date_format),
            "current_month": today.strftime("%m"),
            "last_month": f"{last_month:02d}",
            "current_year": today.strftime("%Y"),
            "last_year": last_year.strftime("%Y"),
            "now_time": now_time.strftime("%H%M%S"),
            "last_day_previous_month": last_day_prev_month.strftime(
                self.dynamic_date_format
            ),
        }

    def _handle_singular_dates(
        self, dynamic_date_marker: str, match: list[tuple], key: str
    ) -> pendulum.DateTime | str | None:  # type: ignore
        """Directs execution of a specific function based on the value of `key`.

        Args:
            match (list[tuple]): list of every pattern match that occurs in a string.
            key (str): Key phrase that determines the execution of a specific function.

        Returns:
            str or pendulum.DateTime:
            - If key == 'x_years_ago_year' returns string of a pendulum date formatted
                with a pendulum token 'Y'.
            - If key != 'x_years_ago_year' returns a pendulum.DateTime
        """
        if key == "last_day_of_month":
            for month_name, year in match:
                replacement = self._process_last_day_of_month(year, month_name)

        elif key == "x_units_ago_full_date":
            for x, units in match:
                replacement = self._get_date_x_ago_full_date(int(x), units)

        elif key == "x_years_ago_year":
            for x in match:
                replacement = self._process_x_years_ago(int(x))  # type: ignore
        else:
            replacement = dynamic_date_marker
        return replacement

    def _generate_dates_based_on_unit(
        self, dynamic_date_marker: str, number: int, unit: str
    ) -> list[str] | str:
        """Direct execution of a specific function based on the value of `unit`.

        Possible values of `unit` correspond to different date formatting styles:
            - 'years': Return a date formatted with a pendulum token 'Y'.
            - 'months': Return a date formatted with a pendulum token 'YMM'.
            - 'days': Return a date with a pendulum token 'YMMDD'.

        Args:
            number (int): The number of units from the current year to include.
            unit (str): The unit of time ('years', 'months', 'days').

        Returns:
            list[str] or string:
                - If the key argument matches the given options, returns
                    a list of dynamically created dates generated based on `unit`
                    in an ascending order.
                - If the key argument doesn't match the given options,
                    returns the unhandled dynamic_date_marker parameter
        """
        if unit == "years":
            return self._generate_years(
                last_years=number, from_year=None, end_year=None
            )
        if unit == "months":
            return self._generate_months(last_months=number)
        if unit == "days":
            return self._generate_dates(last_days=number)
        return dynamic_date_marker

    def _handle_data_ranges(  # noqa: C901
        self, dynamic_date_marker: str, match_found: list[tuple], key: str
    ) -> list[str] | str:
        """Direct execution of a specific function based on the provided value of `key`.

        Depending on a unit ('years'/'months'/'days') the `match_found` refers to,
        date formatting style differs:
            - 'years': Return a date formatted with a pendulum token 'Y'.
            - 'months': Return a date formatted with a pendulum token 'YMM'.
            - 'days': Return a date with a pendulum token 'YMMDD'.

        Args:
            dynamic_date_marker (str): A dynamic date marker that has been found in text
                including the dynamic_date_symbols.
            match_found (list[tuple]): list of every pattern match that occurs in
                a string.
            key (str): Key phrase that determines the execution of a specific function.

        Returns:
            list[str] or string:
                - If the key argument matches the given options,
                    returns list of extracted date ranges in ascending order.
                - If the key argument doesn't match the given options,
                    returns the unhandled dynamic_date_marker parameter
        """
        if key == "last_x_units":
            for number, unit in match_found:
                return self._generate_dates_based_on_unit(
                    dynamic_date_marker, int(number), unit
                )

        elif key == "years_from_x_until_now":
            for start_year in match_found:
                return self._generate_years(
                    last_years=None,
                    from_year=start_year,  # type: ignore
                    end_year=None,  # type: ignore
                )
        elif key == "years_from_x_until_y_years_ago":
            for start_year, end_year in match_found:
                end_year = str(  # noqa: PLW2901
                    int(self.replacements["current_year"]) - int(end_year) + 1
                )
                return self._generate_years(
                    last_years=None,
                    from_year=start_year,  # type: ignore
                    end_year=end_year,  # type: ignore
                )
        elif key == "first_x_days_from":
            for num_days, month_name, year in match_found:
                return self._process_first_days(month_name, year, int(num_days))

        elif key == "last_x_days_from":
            for num_days, month_name, year in match_found:
                return self._process_last_days(month_name, year, int(num_days))

        return dynamic_date_marker

    def _find_dynamic_date_patterns(self, text: str) -> list[str]:
        """Retrieve the matches between the dynamic date symbols from string.

        Args:
            text (str): A string containing dynamic date patterns
                to be listed.

        Returns:
            list: A list of matches found between the dynamic date symbols.
        """
        start_symbol, end_symbol = self.dynamic_date_symbols
        start, end = re.escape(start_symbol), re.escape(end_symbol)
        pattern = rf"{start}.*?{end}"

        return re.findall(pattern, text, re.IGNORECASE)

    def _process_string(self, text: str) -> list[str] | str:
        """Analyze and extract date ranges or singular dates from the given text.

        It bases on specific patterns or pendulum dates.

        Args:
            text (str): A string containing various time-related patterns
                to be analyzed.

        Returns:
            list or string:
                - If the input is a key phrase for a data range,
                    returns list of extracted date ranges in ascending order.
                - If the input is a key phrase for a single date or a pendulum date,
                    returns the input text with an accurate date.
        """
        start_symbol, end_symbol = self.dynamic_date_symbols
        start, end = re.escape(start_symbol), re.escape(end_symbol)
        pattern = rf"{start}.*?{end}"

        matches_between_symbols = re.findall(pattern, text, re.IGNORECASE)
        if not matches_between_symbols:
            return text

        for match in matches_between_symbols:
            match_no_symbols = match[len(start_symbol) : -len(end_symbol)]
            replacement = None
            # Processing the singular dates
            for key, pattern in self.singular_patterns.items():
                match_found = re.findall(pattern, match_no_symbols, re.IGNORECASE)
                if match_found:
                    replacement = self._handle_singular_dates(match, match_found, key)

            # Process range date matches
            for key, pattern in self.range_patterns.items():
                match_found = re.findall(pattern, match_no_symbols, re.IGNORECASE)
                if match_found:
                    return self._handle_data_ranges(match, match_found, key)

            if match_no_symbols in self.replacements:
                replacement = self.replacements[match_no_symbols]
            if not replacement and bool(
                re.match(r"^\s*pendulum\.\w+\(.*\)\s*$", match_no_symbols)
            ):
                replacement = eval(match_no_symbols)  # noqa: S307
            text = text.replace(
                match,
                (
                    replacement.strftime(self.dynamic_date_format)
                    if isinstance(replacement, pendulum.DateTime)  # type: ignore
                    else replacement
                ),
            )

        return text

    def process_dates(self, processed_input: str | list[str]) -> str | list[str]:
        """Process an input by processing dates within it.

        If the input is a string, it applies the _process_string() function.
        If the input is a list, it recursively processes each list item.

        Args:
            processed_input (str or list): The segment to be processed.

        Returns:
            str or list:
                - If processed_input is a string, returns the processed string.
                - If processed_input is a list, returns a list of processed strings.
        """
        if isinstance(processed_input, str):
            return self._process_string(processed_input)
        if isinstance(processed_input, list):
            return [self.process_dates(sub_segment) for sub_segment in processed_input]  # type: ignore
        return processed_input


async def list_block_documents() -> list[Any]:
    """Retrieve list of Prefect block documents."""
    async with PrefectClient(
        api=PREFECT_API_URL.value(), api_key=PREFECT_API_KEY.value()
    ) as client:
        return await client.read_block_documents()


def _get_azure_credentials(secret_name: str) -> dict[str, Any]:
    """Retrieve credentials from the Prefect 'AzureKeyVaultSecretReference' block.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict: A dictionary containing the credentials.
    """
    try:
        credentials = json.loads(
            AzureKeyVaultSecretReference.load(secret_name).get_secret()
        )
    except JSONDecodeError:
        credentials = AzureKeyVaultSecretReference.load(secret_name).get_secret()

    return credentials


def _get_aws_credentials(
    secret_name: str,
    block_type: str,
) -> dict[str, Any] | str:
    """Retrieve credentials from the Prefect 'AwsSecret' block document.

        It distinguishes the types of block to take a correct action.
        This is needed as AwsSecret block stores credentials that can be used in tasks
        which ingest data from different systems (for ex. SAP), where
        AwsCredentials block stores AWS credentials solely and this could
        be needed in other tasks which connects
        to AWS directly ( for ex. redshift spectrum , s3).

    Args:
        secret_name (str): The name of the secret to be retrieved.
        block_type (str): Type of prefect block

    Returns:
        dict | str: A dictionary or a string containing the credentials.
    """
    if block_type == "AwsSecret":
        aws_secret_block = AwsSecret.load(secret_name)
        secret = aws_secret_block.read_secret()
        try:
            credentials = json.loads(secret)
        except json.JSONDecodeError:
            credentials = secret
    elif block_type == "AwsCredentials":
        aws_credentials_block = AwsCredentials.load(secret_name)
        credentials = {
            "aws_access_key_id": aws_credentials_block.aws_access_key_id,
            "aws_secret_access_key": aws_credentials_block.aws_secret_access_key.get_secret_value(),
            "region_name": aws_credentials_block.region_name,
        }

    return credentials


def _get_secret_credentials(secret_name: str) -> dict[str, Any] | str:
    """Retrieve credentials from the Prefect 'Secret' block document.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict | str: A dictionary or a string containing the credentials.
    """
    secret = Secret.load(secret_name).get()
    try:
        credentials = json.loads(secret)
    except json.JSONDecodeError:
        credentials = secret

    return credentials


def _get_database_credentials(secret_name: str) -> dict[str, Any] | str:
    """Retrieve credentials from the Prefect 'DatabaseCredentials' block document.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict | str: A dictionary or a string containing the credentials.
    """
    secret = DatabaseCredentials.load(name=secret_name).dict()

    credentials = secret
    credentials["user"] = secret.get("username")
    credentials["db_name"] = secret.get("database")
    credentials["password"] = secret.get("password").get_secret_value()
    if secret.get("port"):
        credentials["server"] = secret.get("host") + "," + str(secret.get("port"))
    else:
        credentials["server"] = secret.get("host")

    return credentials


def get_credentials(secret_name: str) -> dict[str, Any]:
    """Retrieve credentials from the Prefect block document.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict: A dictionary containing the credentials.
    """
    # Prefect does not allow upper case letters for blocks,
    # so some names might be lowercased versions of the original

    secret_name_lowercase = secret_name.lower()
    blocks = anyio.run(list_block_documents)

    for block in blocks:
        if block.name == secret_name_lowercase:
            block_type = block.block_schema.fields["title"]
            break
    else:
        msg = "The provided secret name is not valid."
        raise MissingPrefectBlockError(msg)

    if block_type in ("AwsSecret", "AwsCredentials"):
        credentials = _get_aws_credentials(secret_name, block_type)
    elif block_type == "AzureKeyVaultSecretReference":
        credentials = _get_azure_credentials(secret_name)
    elif block_type == "DatabaseCredentials":
        credentials = _get_database_credentials(secret_name)
    elif block_type == "Secret":
        credentials = _get_secret_credentials(secret_name)
    else:
        msg = f"The provided secret block type: {block_type} is not supported"
        raise MissingPrefectBlockError(msg)

    return credentials


async def shell_run_command(
    command: str,
    env: dict[str, Any] | None = None,
    helper_command: str | None = None,
    shell: str = "bash",
    return_all: bool = False,
    stream_level: int = logging.INFO,
    logger: logging.Logger | None = None,
    raise_on_failure: bool = True,
) -> list[str] | str:
    """Runs arbitrary shell commands as a util.

    Args:
        command: Shell command to be executed; can also be
            provided post-initialization by calling this task instance.
        env: dictionary of environment variables to use for
            the subprocess; can also be provided at runtime.
        helper_command: String representing a shell command, which
            will be executed prior to the `command` in the same process.
            Can be used to change directories, define helper functions, etc.
            for different commands in a flow.
        shell: Shell to run the command with; defaults to "bash".
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string; defaults to `False`.
        stream_level: The logging level of the stream.
        logger: Can pass a desired logger; if not passed, will automatically
            gets a run logger from Prefect.
        raise_on_failure: Whether to raise an exception if the command fails.

    Returns:
        If return all, returns all lines as a list; else the last line as a string.

    Example:
        Echo "hey it works".
        ```python
        from prefect_shell.utils import shell_run_command
        await shell_run_command("echo hey it works")
        ```
    """
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("prefect_shell.utils")

    current_env = os.environ.copy()
    current_env.update(env or {})

    with tempfile.NamedTemporaryFile(prefix="prefect-") as tmp:
        if helper_command:
            tmp.write(helper_command.encode())
            tmp.write(os.linesep.encode())
        tmp.write(command.encode())
        tmp.flush()

        shell_command = [shell, tmp.name]
        if sys.platform == "win32":
            shell_command = " ".join(shell_command)

        lines = []
        async with await open_process(shell_command, env=env) as process:
            async for text in TextReceiveStream(process.stdout):
                logger.log(stream_level, text)
                lines.extend(text.rstrip().split("\n"))

            await process.wait()
            if process.returncode:
                stderr = "\n".join(
                    [text async for text in TextReceiveStream(process.stderr)]
                )
                if not stderr and lines:
                    stderr = f"{lines[-1]}\n"
                msg = f"Command failed with exit code {process.returncode}:\n{stderr}"
                if raise_on_failure:
                    raise RuntimeError(msg)
                lines.append(msg)

    return lines if return_all else lines[-1]
