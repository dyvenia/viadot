import pytest

from viadot.tasks import ReadAzureKeyVaultSecret


class TestAzureKeyVault:
    def test_initialization(self):
        task = ReadAzureKeyVaultSecret("test")

    def test_initialization_passes_to_task_constructor(self):
        task = ReadAzureKeyVaultSecret(name="test", tags=["Azure"])
        assert task.name == "test"
        assert task.tags == {"Azure"}

    def test_raises_if_secret_not_eventually_provided(self):
        task = ReadAzureKeyVaultSecret()

        with pytest.raises(ValueError, match="secret"):
            task.run()
