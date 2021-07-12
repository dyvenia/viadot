from .base import SQL


class SQLite(SQL):
    """A SQLite source

    Args:
        server (str): server string, usually localhost
        db (str): the file path to the db e.g. /home/somedb.sqlite
    """

    def __init__(
        self,
        query_timeout: int = 60,
        *args,
        **kwargs,
    ):
        super().__init__(
            *args,
            driver="/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so",
            query_timeout=query_timeout,
            **kwargs,
        )
        self.credentials["server"] = "localhost"

    @property
    def conn_str(self):
        """Generate a connection string from params or config.
        Note that the user and password are escapedd with '{}' characters.

        Returns:
            str: The ODBC connection string.
        """
        driver = self.credentials["driver"]
        server = self.credentials["server"]
        db_name = self.credentials["db_name"]

        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db_name};"

        return conn_str
