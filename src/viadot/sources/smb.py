from pydantic import BaseModel
import smbclient

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source


class SMBCredentials(BaseModel):
    username: str  # username (e.g username@{tenant_name}.com)
    password: str

    @root_validator(pre=True)
    def is_configured(cls, credentials: dict) -> dict:  # noqa: N805
        """Validate that both username and password are provided.

        This method is a Pydantic root validator that checks if both
        username and password fields are present and non-empty.

        Args:
            credentials (dict): A dictionary containing the credential fields.

        Returns:
            dict: The validated credentials dictionary.

        Raises:
            CredentialError: If either username or password is missing.
        """
        username = credentials.get("username")
        password = credentials.get("password")

        if not (username and password):
            msg = "'username', and 'password' credentials are required."
            raise CredentialError(msg)
        return credentials


class SMB(Source):
    def __init__(
        self,
        base_path: str,
        credentials: SMBCredentials = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the SMB with a base path.

        Args:
            base_path (str): The root directory to start scanning from.
            credentials (SMBCredentials): Sharepoint credentials.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials.
        """
        self.base_path = base_path
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = dict(SMBCredentials(**raw_creds))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        smbclient.ClientConfig(
            username=self.credentials.get("username"),
            password=self.credentials.get("password"),
        )
