import io

import pandas as pd
import paramiko
import pymysql
from sshtunnel import SSHTunnelForwarder

from viadot.config import local_config
from viadot.exceptions import CredentialError
from viadot.sources.base import Source


class MySQL(Source):
    def __init__(
        self,
        config_key: str = None,
        credentials: dict = None,
        *args,
        **kwargs,
    ):

        """
        A class for interacting with DuckDB.

        Args:
            config_key (str, optional): The key inside local config containing the config.
            User can choose to use this or pass credentials directly to the `credentials`
            parameter. Defaults to None.
            credentials (dict, optional): Credentials for the connection. Defaults to None.
        """

        if config_key:
            config_credentials = local_config.get(config_key)

        credentials = credentials if credentials else config_credentials
        if credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

    @property
    def con(self) -> pymysql.connect:
        """
        Return a new connection to the MySQL database.

        Returns:
            pymysql.connect: database connection.
        """

        if self.credentials.get("host") is None:
            host = "127.0.0.1"

        conn = pymysql.connect(
            host=host,
            user=self.credentials.get("sql_username"),
            passwd=self.credentials.get("sql_password"),
            db=self.credentials.get("sql_main_database"),
            port=self.credentials.get("sql_port"),
        )
        return conn

    def to_df(self, query: str) -> pd.DataFrame:
        """
        Get DataFrame from MySQL database by pandas and SQL query.

        Returns:
            pd.DataFrame: Pandas dataframe.
        """
        data = pd.read_sql_query(query, self.con)
        self.con.close()
        return data

    def connect_sql_ssh(
        self,
        query: str,
    ) -> pd.DataFrame:
        """
        Establish connection with database using Secure Shell Protocol (SSH) and
        get pandas DataFrame by proper SQL query.

        Returns:
            pd.DataFrame: Pandas dataframe.
        """

        if self.credentials.get("host") is None:
            host = "127.0.0.1"

        sql_hostname = self.credentials.get("sql_hostname")
        sql_username = self.credentials.get("sql_username")
        sql_password = self.credentials.get("sql_password")
        sql_db_name = self.credentials.get("sql_db_name")
        sql_port = self.credentials.get("sql_port")

        ssh_host = self.credentials.get("ssh_host")
        ssh_user = self.credentials.get("ssh_user")
        ssh_port = self.credentials.get("ssh_port")
        rsakey = self.credentials.get("rsakey")

        rsakey = self.credentials["rsakey"]
        ssh_pkey = paramiko.RSAKey.from_private_key(io.StringIO(rsakey))

        with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_pkey=ssh_pkey,
            remote_bind_address=(sql_hostname, sql_port),
        ) as ssh_tunnel:

            conn = pymysql.connect(
                host=host,
                user=sql_username,
                passwd=sql_password,
                db=sql_db_name,
                port=ssh_tunnel.local_bind_port,
            )

            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
