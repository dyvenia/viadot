import uuid

import pytest
from azure.core.exceptions import ResourceNotFoundError

from viadot.tasks import (
    AzureKeyVaultSecret,
    CreateAzureKeyVaultSecret,
    DeleteAzureKeyVaultSecret,
)

uuid4 = uuid.uuid4()
SECRET_NAME = f"test-secret-{uuid4}"
SECRET_VALUE = "test_value"


def test_create_azure_key_vault_secret():
    create_azure_key_vault_task = CreateAzureKeyVaultSecret()
    was_successful = create_azure_key_vault_task.run(
        secret=SECRET_NAME, value=SECRET_VALUE, lifetime=0.0005
    )
    assert was_successful


def test_read_azure_key_vault_secret():
    azure_key_vault_task = AzureKeyVaultSecret()
    value = azure_key_vault_task.run(secret=SECRET_NAME)
    assert value == SECRET_VALUE


def test_delete_azure_key_vault_secret():
    delete_azure_key_vault_task = DeleteAzureKeyVaultSecret()
    was_successful = delete_azure_key_vault_task.run(secret=SECRET_NAME)

    assert was_successful

    with pytest.raises(ResourceNotFoundError):
        azure_key_vault_task = AzureKeyVaultSecret()
        azure_key_vault_task.run(secret=SECRET_NAME)
