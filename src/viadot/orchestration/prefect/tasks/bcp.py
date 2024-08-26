import subprocess
from typing import Any, Literal

from prefect import task

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials


@task
def bcp(
    path: str = None,
    schema: str = None,
    table: str = None,
    chunksize: int = 5000,
    error_log_file_path: str = "./log_file.log",
    on_error: Literal["skip", "fail"] = "skip",
    credentials_secret: str | None = None,
    config_key: str | None = None,
    credentials: dict[str, Any] | None = None,
):
    """Upload data from a CSV file into an SQLServer table using BCP.
        For more information on bcp (bulk copy program), see
            https://learn.microsoft.com/en-us/sql/tools/bcp-utility.

    Args:
        path (str):  Where to store the CSV data dump used for bulk upload to a database.
        schema (str, optional): Destination schema. Defaults to None.
        table (str, optional): Destination table. Defaults to None.
        chunksize (int, optional): Size of a chunck to use in the bcp function.
            Defaults to 5000.
        error_log_file_path (string, optional): Full path of an error file. Defaults
            to "./log_file.log".
        on_error (str, optional): What to do in case of a bcp error. Defaults to "skip".
        credentials_secret (str, optional): The name of the secret storing
            the credentialsto the SQLServer. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        credentials (dict[str, Any], optional): Credentials to the SQLServer.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials to the SQLServer. Defaults to None.
    """
    if not (credentials_secret or credentials or config_key):
        raise MissingSourceCredentialsError

    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )
    fqn = f"{schema}.{table}" if schema else table
    server = credentials["server"]
    db_name = credentials["db_name"]
    uid = credentials["user"]
    pwd = credentials["password"]

    if "," in server:
        # A space after the comma is allowed in the ODBC connection string
        # but not in BCP's 'server' argument.
        server = server.replace(" ", "")

    if on_error == "skip":
        max_error = 0
    elif on_error == "fail":
        max_error = 1
    else:
        raise ValueError(
            "Please provide correct 'on_error' parameter value - 'skip' or 'fail'. "
        )
    bcp_command = [
        "/opt/mssql-tools/bin/bcp",
        fqn,
        "in",
        path,
        "-S",
        server,
        "-d",
        db_name,
        "-U",
        uid,
        "-P",
        pwd,
        "-b",
        str(chunksize),
        "-m",
        str(max_error),
        "-c",
        "-v",
        "-e",
        error_log_file_path,
        "-h",
        "TABLOCK",
        "-F",
        "2",
    ]

    result = subprocess.run(bcp_command, capture_output=True, text=True, check=False)
    return result
