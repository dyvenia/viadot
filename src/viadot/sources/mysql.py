"""A connector for MySQL, accessed through an ODBC driver."""

from typing import Any

from pydantic import BaseModel, SecretStr

from viadot.config import get_source_credentials
from viadot.sources.base import SQL


class MySQLCredentials(BaseModel):
    user: str
    password: str | SecretStr | None = None
    server: str
    port: int = 3306
    db_name: str
    driver: str = "MariaDB Unicode"
    sslmode: str | None = None


class MySQL(SQL):
    """MySQL connector class.

    The ODBC driver named in ``driver`` must be installed in the environment and
    listed in ``odbcinst.ini``. The default ``MariaDB Unicode`` is the MariaDB
    Connector/ODBC driver, which also connects to MySQL servers.
    """

    def __init__(
        self,
        credentials: dict[str, Any] | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the MySQL connector.

        Args:
            credentials (dict[str, Any], optional): Connection settings with keys
                ``user``, ``password``, ``server``, ``port``, ``db_name``,
                ``driver`` and optionally ``sslmode``. Defaults to None.
            config_key (str, optional): The viadot config key from which to read
                the credentials. Defaults to None.
        """
        raw_creds = (
            credentials
            or (get_source_credentials(config_key) if config_key else None)
            or {}
        )
        validated_creds = MySQLCredentials(**raw_creds).model_dump()

        super().__init__(*args, credentials=validated_creds, **kwargs)

    @property
    def conn_str(self) -> str:
        """Generate an ODBC connection string for MySQL.

        Returns:
            str: The ODBC connection string.
        """
        pwd = self.credentials.get("password") or ""
        if isinstance(pwd, SecretStr):
            pwd = pwd.get_secret_value()
        # Braces protect ';' and '=' inside the value; '}' is escaped as '}}'.
        pwd = "{" + pwd.replace("}", "}}") + "}"

        conn_str = (
            f"DRIVER={{{self.credentials['driver']}}};"
            f"SERVER={self.credentials['server']};"
            f"PORT={self.credentials['port']};"
            f"DATABASE={self.credentials['db_name']};"
            f"UID={self.credentials['user']};"
            f"PWD={pwd};"
        )
        # MariaDB ODBC 3.1 ignores SSLMODE. required is FORCETLS.
        sslmode = (self.credentials.get("sslmode") or "").strip().lower()
        if sslmode == "required":
            conn_str += "FORCETLS=1;"
        conn_str += "CONN_TIMEOUT=30;"
        return conn_str
