import os
from datetime import timedelta

import pendulum
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.secrets.base import SecretBase
from prefect.utilities.tasks import defaults_from_attrs


def get_key_vault(
    credentials: str, secret_client_kwargs: dict, vault_name: str = None
) -> SecretClient:
    if not vault_name:
        vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()
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


class AzureKeyVaultSecret(SecretBase):
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
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        **kwargs,
    ):
        self.secret = secret
        self.vault_name = vault_name

        if secret_client_kwargs is None:
            self.secret_client_kwargs = {}
        else:
            self.secret_client_kwargs = secret_client_kwargs

        super().__init__(max_retries=max_retries, retry_delay=retry_delay, **kwargs)

    @defaults_from_attrs("secret", "vault_name")
    def run(
        self,
        secret: str = None,
        vault_name: str = None,
        credentials: dict = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
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
            from viadot.tasks import AzureKeyVaultSecret

            azure_secret_task = AzureKeyVaultSecret()

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
        lifetime: int = 365,
        vault_name: str = None,
        secret_client_kwargs: dict = None,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        **kwargs,
    ):
        self.secret = secret
        self.value = value
        self.lifetime = lifetime
        self.vault_name = vault_name

        if secret_client_kwargs is None:
            self.secret_client_kwargs = {}
        else:
            self.secret_client_kwargs = secret_client_kwargs

        super().__init__(max_retries=max_retries, retry_delay=retry_delay, **kwargs)

    @defaults_from_attrs(
        "secret",
        "value",
        "lifetime",
        "vault_name",
    )
    def run(
        self,
        secret: str = None,
        value: str = None,
        lifetime: int = None,
        vault_name: str = None,
        credentials: dict = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> bool:
        """
        Task run method.

        Args:
            - secret (str): the name of the secret to set
            - value (str): the value which the secret will hold
            - lifetime (int): The number of days after which the secret should expire.
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
            - bool: Whether the secret was created successfully.
        """

        if secret is None:
            raise ValueError("A secret name must be provided.")

        key_vault = get_key_vault(
            vault_name=vault_name,
            credentials=credentials,
            secret_client_kwargs=self.secret_client_kwargs,
        )

        expires_on = pendulum.now("UTC").add(days=lifetime)
        secret_obj = key_vault.set_secret(secret, value, expires_on=expires_on)
        was_successful = secret_obj.name == secret

        return was_successful


class DeleteAzureKeyVaultSecret(SecretBase):
    """
    Task for removing ("soft delete") a secret from an Azure Key Vault.
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
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        **kwargs,
    ):
        self.secret = secret
        self.vault_name = vault_name

        if secret_client_kwargs is None:
            self.secret_client_kwargs = {}
        else:
            self.secret_client_kwargs = secret_client_kwargs

        super().__init__(max_retries=max_retries, retry_delay=retry_delay, **kwargs)

    @defaults_from_attrs("secret", "vault_name")
    def run(
        self,
        secret: str = None,
        vault_name: str = None,
        credentials: dict = None,
        max_retries: int = None,
        retry_delay: timedelta = None,
    ) -> bool:
        """
        Task run method.

        Args:
            - secret (str): the name of the secret to delete
            - vault_name (str): the name of the vault whethe the secret is located
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
            from viadot.tasks import DeleteAzureKeyVaultSecret

            azure_secret_task = DeleteAzureKeyVaultSecret()

            with Flow(name="example") as f:
                secret = azure_secret_task(secret="test", vault_name="my_vault_name")
            out = f.run()
            ```
        Returns:
            - bool: Whether the secret was deleted successfully.
        """

        if secret is None:
            raise ValueError("A secret name must be provided.")

        key_vault = get_key_vault(
            vault_name=vault_name,
            credentials=credentials,
            secret_client_kwargs=self.secret_client_kwargs,
        )

        poller = key_vault.begin_delete_secret(secret)
        poller.wait(timeout=60 * 5)
        was_successful = poller.status() == "finished"

        return was_successful
