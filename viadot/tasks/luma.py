import json

from prefect.tasks.shell import ShellTask

from .azure_key_vault import AzureKeyVaultSecret


class LumaIngest(ShellTask):
    def __init__(
        self,
        metadata_dir_path: str,
        endpoint: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        if not endpoint:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=credentials_secret, vault_name=vault_name
            )
            endpoint = json.loads(credentials_str).get("endpoint")
        self.endpoint = endpoint
        self.metadata_dir_path = metadata_dir_path
        self.command = f"luma dbt ingest {metadata_dir_path} --endpoint {endpoint}"
        self.return_all = True
        self.stream_output = True
        self.log_stderr = True
