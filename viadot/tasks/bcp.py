import json

from prefect.tasks.shell import ShellTask
from viadot.tasks import ReadAzureKeyVaultSecret


class BCPTask(ShellTask):
    def __init__(self, *args, **kwargs):
        super().__init__(name="bcp", log_stderr=True, return_all=True, *args, **kwargs)

    def run(
        self,
        path: str = None,
        schema: str = None,
        table: str = None,
        credentials: dict = None,
        credentials_secret: str = None,
        vault_name: str = None,
        **kwargs,
    ):
        azure_secret_task = ReadAzureKeyVaultSecret()
        credentials_str = azure_secret_task.run(
            secret=credentials_secret, vault_name=vault_name
        )
        credentials = json.loads(credentials_str)

        fqn = f"{schema}.{table}" if schema else table

        server = credentials["server"]
        db_name = credentials["db_name"]
        uid = credentials["user"]
        pwd = credentials["password"]

        command = f"/opt/mssql-tools/bin/bcp {fqn} in {path} -S {server} -d {db_name} -U {uid} -P '{pwd}' -c -F 2"
        return super().run(command=command, **kwargs)
