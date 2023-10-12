import json
from prefect.tasks.shell import ShellTask
from .azure_key_vault import AzureKeyVaultSecret


class LumaIngest(ShellTask):
    """Ingest metadata into [Luma](https://github.com/dyvenia/luma)."""

    def __init__(
        self,
        metadata_dir_path: str,
        url: str = None,
        dbt_project_path: str = None,
        credentials_secret: str = None,
        vault_name: str = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Runs Luma ingestion by sending dbt artifacts to Luma ingestion API.

        Args:
            metadata_dir_path (str): The path to the directory containing metadata files.
                In the case of dbt, it's dbt project's `target` directory, which contains dbt artifacts
                (`sources.json`, `catalog.json`, `manifest.json`, and `run_results.json`).
            url (str, optional): The url of the Luma ingestion API. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing Luma credentials.
                Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
        """
        super().__init__(*args, **kwargs)

        if not url:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=credentials_secret, vault_name=vault_name
            )
            url = json.loads(credentials_str).get("url")
        self.helper_script = dbt_project_path
        self.url = url
        self.metadata_dir_path = metadata_dir_path
        self.command = f"luma dbt send-test-results --luma-url {url} --metadata-dir {metadata_dir_path}"
        self.return_all = True
        self.stream_output = True
        self.log_stderr = True
