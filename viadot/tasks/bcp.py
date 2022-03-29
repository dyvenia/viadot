import json
from datetime import timedelta

from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.shell import ShellTask
from prefect.utilities.tasks import defaults_from_attrs

from .azure_key_vault import AzureKeyVaultSecret


class BCPTask(ShellTask):
    """
    Task for bulk inserting data into SQL Server-compatible databases.

    Args:
        - path (str, optional): The path to the local CSV file to be inserted.
        - schema (str, optional): The destination schema.
        - table (str, optional): The destination table.
        - chunksize (int, optional): The chunk size to use.
        - credentials (dict, optional): The credentials to use for connecting with the database.
        - vault_name (str): The name of the vault from which to fetch the secret.
        - **kwargs (dict, optional): Additional keyword arguments to pass to the Task constructor.
    """

    def __init__(
        self,
        path: str = None,
        schema: str = None,
        table: str = None,
        chunksize: int = 5000,
        credentials: dict = None,
        vault_name: str = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        self.path = path
        self.schema = schema
        self.table = table
        self.chunksize = chunksize
        self.credentials = credentials
        self.vault_name = vault_name

        super().__init__(
            name="bcp",
            log_stderr=True,
            return_all=True,
            max_retries=max_retries,
            retry_delay=retry_delay,
            *args,
            **kwargs,
        )

    @defaults_from_attrs(
        "path",
        "schema",
        "table",
        "chunksize",
        "credentials",
        "vault_name",
        "max_retries",
        "retry_delay",
    )
    def run(
        self,
        path: str = None,
        schema: str = None,
        table: str = None,
        chunksize: int = None,
        credentials: dict = None,
        credentials_secret: str = None,
        vault_name: str = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
        **kwargs,
    ) -> str:
        """
        Task run method.

        Args:
        - path (str, optional): The path to the local CSV file to be inserted.
        - schema (str, optional): The destination schema.
        - table (str, optional): The destination table.
        - chunksize (int, optional): The chunk size to use. By default 5000.
        - credentials (dict, optional): The credentials to use for connecting with SQL Server.
        - credentials_secret (str, optional): The name of the Key Vault secret containing database credentials.
        (server, db_name, user, password)
        - vault_name (str): The name of the vault from which to fetch the secret.

        Returns:
            str: The output of the bcp CLI command.
        """
        if not credentials:
            if not credentials_secret:
                # attempt to read a default for the service principal secret name
                try:
                    credentials_secret = PrefectSecret(
                        "AZURE_DEFAULT_SQLDB_SERVICE_PRINCIPAL_SECRET"
                    ).run()
                except ValueError:
                    pass

            if credentials_secret:
                credentials_str = AzureKeyVaultSecret(
                    credentials_secret, vault_name=vault_name
                ).run()
                credentials = json.loads(credentials_str)

        fqn = f"{schema}.{table}" if schema else table

        server = credentials["server"]
        db_name = credentials["db_name"]
        uid = credentials["user"]
        pwd = credentials["password"]

        if "," in server:
            # A space after the comma is allowed in the ODBC connection string
            # but not in BCP's 'server' argument.
            server = server.replace(" ", "")

        command = f"/opt/mssql-tools/bin/bcp {fqn} in '{path}' -S {server} -d {db_name} -U {uid} -P '{pwd}' -c -F 2 -b {chunksize} -h 'TABLOCK'"
        return super().run(command=command, **kwargs)
