import json

from prefect.tasks.shell import ShellTask

from .azure_key_vault import AzureKeyVaultSecret


class LumaIngest(ShellTask):
    """Tasks for interacting with [Luma](https://github.com/dyvenia/luma)."""

    def __init__(
        self,
        metadata_dir_path: str,
        endpoint: str = None,
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
            endpoint (str, optional): The endpoint of the Luma ingestion API. Defaults to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a Luma credentials.
                Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.
        """
        super().__init__(*args, **kwargs)

        if not endpoint:
            azure_secret_task = AzureKeyVaultSecret()
            credentials_str = azure_secret_task.run(
                secret=credentials_secret, vault_name=vault_name
            )
            endpoint = json.loads(credentials_str).get("endpoint")
        self.endpoint = endpoint
        self.metadata_dir_path = metadata_dir_path
        self.command = (
            f"luma dbt ingest --metadata-dir {metadata_dir_path} --luma-url {endpoint}"
        )
        self.return_all = True
        self.stream_output = True
        self.log_stderr = True
