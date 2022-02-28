from datetime import timedelta
from typing import Any, Dict, Literal

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from ..config import local_config
from ..sources import SQLServer


class SQLServerCreateTable(Task):
    """
    Create a table in SQL Server.

    Args:
        schema (str, optional): Destination schema.
        table (str, optional): Destination table.
        dtypes (Dict[str, Any], optional): Data types to enforce.
        if_exists (Literal, optional): What to do if the table already exists.
        credentials (dict, optional): Credentials for the connection.
    """

    def __init__(
        self,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace", "skip", "delete"] = "fail",
        credentials: dict = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.schema = schema
        self.table = table
        self.dtypes = dtypes
        self.if_exists = if_exists
        self.credentials = credentials
        super().__init__(
            name="sql_server_create_table",
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    @defaults_from_attrs("if_exists")
    def run(
        self,
        schema: str = None,
        table: str = None,
        dtypes: Dict[str, Any] = None,
        if_exists: Literal["fail", "replace", "skip", "delete"] = None,
        credentials: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ):
        """
        Create a table in SQL Server.

        Args:
            schema (str, optional): Destination schema.
            table (str, optional): Destination table.
            dtypes (Dict[str, Any], optional): Data types to enforce.
            if_exists (Literal, optional): What to do if the table already exists.
            credentials (dict, optional): Credentials for the connection.
        """

        if credentials is None:
            credentials = local_config.get("SQL_SERVER").get("DEV")
        sql_server = SQLServer(credentials=credentials)

        fqn = f"{schema}.{table}" if schema is not None else table
        created = sql_server.create_table(
            schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
        )
        if created:
            self.logger.info(f"Successfully created table {fqn}.")
        else:
            self.logger.info(
                f"Table {fqn} has not been created as if_exists is set to {if_exists}."
            )
