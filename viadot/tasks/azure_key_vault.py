import os

from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from prefect.tasks.secrets.base import SecretBase
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.tasks import defaults_from_attrs


def get_key_vault(
    vault_name: str, credentials: str, secret_client_kwargs: dict
) -> SecretClient:
    if credentials:
        # set credentials as env variables so that they're discoverable by EnvironmentCredential
        key_vault_credentials = credentials["KEY_VAULT"][vault_name]
        os.environ["AZURE_TENANT_ID"] = key_vault_credentials["AZURE_TENANT_ID"]
        os.environ["AZURE_CLIENT_ID"] = key_vault_credentials["AZURE_CLIENT_ID"]
        os.environ["AZURE_CLIENT_SECRET"] = key_vault_credentials["AZURE_CLIENT_SECRET"]
    else:
        try:
            # we can read the credentials automatically if user uses the default name
            credentials = PrefectSecret("AZURE_CREDENTIALS").run()
            key_vault_credentials = credentials["KEY_VAULT"][vault_name]
            os.environ["AZURE_TENANT_ID"] = key_vault_credentials["AZURE_TENANT_ID"]
            os.environ["AZURE_CLIENT_ID"] = key_vault_credentials["AZURE_CLIENT_ID"]
            os.environ["AZURE_CLIENT_SECRET"] = key_vault_credentials[
                "AZURE_CLIENT_SECRET"
            ]
        except ValueError as e:
            # go to step 3 (attempt to read from env)
            pass
    credentials = EnvironmentCredential()
    vault_url = f"https://{vault_name}.vault.azure.net"
    key_vault = SecretClient(
        vault_url=vault_url, credential=credentials, **secret_client_kwargs
    )
    return key_vault


class ReadAzureKeyVaultSecret(SecretBase):
    """
    Task for retrieving secrets from an Azure Key Vault and returning it as a dictionary.
    Note that all initialization arguments can optionally be provided or overwritten at runtime.

    For authentication, there are two options: you can set the `AZURE_CREDENTIALS` Prefect Secret
    containing your Azure Key Vault credentials which will be passed directly to `SecretClient`, or you
    can [configure your flow's runtime
    environment](https://docs.microsoft.com/en-us/dotnet/api/azure.identity.environmentcredential?view=azure-dotnet)
    for `EnvironmentCredential`.

    Args:
        - secret (str, optional): the name of the secret to retrieve
        - vault_name (str): the name of the vault from which to fetch the secret
        - secret_client_kwargs (dict, optional): additional keyword arguments to forward to the SecretClient.
        - **kwargs (dict, optional): additional keyword arguments to pass to the Task constructor
    """

    def __init__(
        self,
        secret: str = None,
        vault_name: str = None,
        secret_client_kwargs: dict = None,
        **kwargs,
    ):
        self.secret = secret
        self.vault_name = vault_name

        if secret_client_kwargs is None:
            self.secret_client_kwargs = {}
        else:
            self.secret_client_kwargs = secret_client_kwargs

        super().__init__(**kwargs)

    @defaults_from_attrs("secret", "vault_name")
    def run(
        self, secret: str = None, vault_name: str = None, credentials: dict = None
    ) -> str:
        """
        Task run method.

        Args:
            - secret (str): the name of the secret to retrieve
            - vault_name (str): the name of the vault from which to fetch the secret
            - credentials (dict, optional): your Azure Key Vault credentials passed from an upstream
                Secret task. By default, credentials are read from the `AZURE_CREDENTIALS`
                Prefect Secret; this Secret must be a JSON string
                with the subkey `KEY_VAULT` and then vault_name containing three keys:
                `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`, which will be
                passed directly to `SecretClient`.  If not provided here or in context, the task
                will fall back to Azure credentials discovery using `EnvironmentCredential()`.
                Example `AZURE_CREDENTIALS` environment variable:
                `export AZURE_CREDENTIALS = '{"KEY_VAULT": {"test_key_vault": {"AZURE_TENANT_ID": "a", "AZURE_CLIENT_ID": "b", "AZURE_CLIENT_SECRET": "c"}}}'`

        Example:
            ```python
            from prefect import Flow
            from viadot.tasks import ReadAzureKeyVaultSecret

            azure_secret_task = ReadAzureKeyVaultSecret()

            with Flow(name="example") as f:
                secret = azure_secret_task(secret="test", vault_name="my_vault_name")
            out = f.run()
            ```
        Returns:
            - str: the contents of this secret, as a string
        """

        if secret is None:
            raise ValueError("A secret name must be provided.")

        key_vault = get_key_vault(
            vault_name=vault_name,
            credentials=credentials,
            secret_client_kwargs=self.secret_client_kwargs,
        )

        secret_string = key_vault.get_secret(secret).value

        return secret_string


class CreateAzureKeyVaultSecret(SecretBase):
    """
    Task for creating secrets in an Azure Key Vault.
    Note that all initialization arguments can optionally be provided or overwritten at runtime.

    For authentication, there are two options: you can set the `AZURE_CREDENTIALS` Prefect Secret
    containing your Azure Key Vault credentials which will be passed directly to `SecretClient`, or you
    can [configure your flow's runtime
    environment](https://docs.microsoft.com/en-us/dotnet/api/azure.identity.environmentcredential?view=azure-dotnet)
    for `EnvironmentCredential`.

    Args:
        - secret (str, optional): the name of the secret to retrieve
        - vault_name (str): the name of the vault from which to fetch the secret
        - secret_client_kwargs (dict, optional): additional keyword arguments to forward to the SecretClient.
        - **kwargs (dict, optional): additional keyword arguments to pass to the Task constructor
    """

    def __init__(
        self,
        secret: str = None,
        value: str = None,
        vault_name: str = None,
        secret_client_kwargs: dict = None,
        **kwargs,
    ):
        self.secret = secret
        self.value = value
        self.vault_name = vault_name

        if secret_client_kwargs is None:
            self.secret_client_kwargs = {}
        else:
            self.secret_client_kwargs = secret_client_kwargs

        super().__init__(**kwargs)

    @defaults_from_attrs("secret", "value", "vault_name")
    def run(
        self,
        secret: str = None,
        value: str = None,
        vault_name: str = None,
        credentials: dict = None,
    ) -> str:
        """
        Task run method.

        Args:
            - secret (str): the name of the secret to set
            - value (str): the value which the secret will hold
            - vault_name (str): the name of the vault from which to fetch the secret
            - credentials (dict, optional): your Azure Key Vault credentials passed from an upstream
                Secret task; this Secret must be a JSON string
                with the subkey `KEY_VAULT` and then vault_name containing three keys:
                `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`, which will be
                passed directly to `SecretClient`.  If not provided here or in context, the task
                will fall back to Azure credentials discovery using `EnvironmentCredential()`.
                Example `AZURE_CREDENTIALS` environment variable:
                `export AZURE_CREDENTIALS = '{"KEY_VAULT": {"test_key_vault": {"AZURE_TENANT_ID": "a", "AZURE_CLIENT_ID": "b", "AZURE_CLIENT_SECRET": "c"}}}'`

        Example:
            ```python
            from prefect import Flow
            from prefect.tasks.secrets import PrefectSecret
            from viadot.tasks import CreateAzureKeyVaultSecret

            create_secret_task = CreateAzureKeyVaultSecret()

            with Flow(name="example") as f:
                azure_credentials = PrefectSecret("AZURE_CREDENTIALS")
                secret = create_secret_task(secret="test2", value=42, vault_name="my_vault_name", credentials=azure_credentials)
            out = f.run()
            ```
        Returns:
            - str: the contents of this secret, as a string
        """

        if secret is None:
            raise ValueError("A secret name must be provided.")

        key_vault = get_key_vault(
            vault_name=vault_name,
            credentials=credentials,
            secret_client_kwargs=self.secret_client_kwargs,
        )

        secret_string = key_vault.set_secret(secret, value)

        return secret_string
