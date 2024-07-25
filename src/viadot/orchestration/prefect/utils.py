"""Common utilities for use in tasks."""

import contextlib
import json
import logging
import os
import sys
import tempfile
from json.decoder import JSONDecodeError
from typing import Any

import anyio
from anyio import open_process
from anyio.streams.text import TextReceiveStream
from prefect.blocks.system import Secret
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL
from prefect_aws.secrets_manager import AwsSecret
from prefect_sqlalchemy import DatabaseCredentials

from viadot.orchestration.prefect.exceptions import MissingPrefectBlockError

with contextlib.suppress(ModuleNotFoundError):
    from prefect_azure import AzureKeyVaultSecretReference


async def list_block_documents() -> list[Any]:
    """Retrieve list of Prefect block documents."""
    async with PrefectClient(
        api=PREFECT_API_URL.value(), api_key=PREFECT_API_KEY.value()
    ) as client:
        return await client.read_block_documents()


def _get_azure_credentials(secret_name: str) -> dict[str, Any]:
    """Retrieve credentials from the Prefect 'AzureKeyVaultSecretReference' block.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict: A dictionary containing the credentials.
    """
    try:
        credentials = json.loads(
            AzureKeyVaultSecretReference.load(secret_name).get_secret()
        )
    except JSONDecodeError:
        credentials = AzureKeyVaultSecretReference.load(secret_name).get_secret()

    return credentials


def _get_aws_credentials(secret_name: str) -> dict[str, Any] | str:
    """Retrieve credentials from the Prefect 'AwsSecret' block document.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict | str: A dictionary or a string containing the credentials.
    """

    aws_secret_block = AwsSecret.load(secret_name)
    credentials = aws_secret_block.read_secret()
    credentials = json.loads(credentials)

    return credentials


def _get_secret_credentials(secret_name: str) -> dict[str, Any] | str:
    """Retrieve credentials from the Prefect 'Secret' block document.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict | str: A dictionary or a string containing the credentials.
    """
    secret = Secret.load(secret_name).get()
    try:
        credentials = json.loads(secret)
    except json.JSONDecodeError:
        credentials = secret

    return credentials


def _get_database_credentials(secret_name: str) -> dict[str, Any] | str:
    """Retrieve credentials from the Prefect 'DatabaseCredentials' block document.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict | str: A dictionary or a string containing the credentials.
    """
    secret = DatabaseCredentials.load(name=secret_name).dict()

    credentials = secret
    credentials["user"] = secret.get("username")
    credentials["db_name"] = secret.get("database")
    credentials["password"] = secret.get("password").get_secret_value()
    if secret.get("port"):
        credentials["server"] = secret.get("host") + "," + str(secret.get("port"))
    else:
        credentials["server"] = secret.get("host")

    return credentials


def get_credentials(secret_name: str) -> dict[str, Any]:
    """Retrieve credentials from the Prefect block document.

    Args:
        secret_name (str): The name of the secret to be retrieved.

    Returns:
        dict: A dictionary containing the credentials.
    """
    # Prefect does not allow upper case letters for blocks,
    # so some names might be lowercased versions of the original

    secret_name_lowercase = secret_name.lower()
    blocks = anyio.run(list_block_documents)

    for block in blocks:
        if block.name == secret_name_lowercase:
            block_type = block.block_schema.fields["title"]
            break
    else:
        msg = "The provided secret name is not valid."
        raise MissingPrefectBlockError(msg)

    if block_type == "AwsSecret":
        credentials = _get_aws_credentials(secret_name)
    elif block_type == "AzureKeyVaultSecretReference":
        credentials = _get_azure_credentials(secret_name)
    elif block_type == "DatabaseCredentials":
        credentials = _get_database_credentials(secret_name)
    elif block_type == "Secret":
        credentials = _get_secret_credentials(secret_name)
    else:
        msg = f"The provided secret block type: {block_type} is not supported"
        raise MissingPrefectBlockError(msg)

    return credentials


async def shell_run_command(  # noqa: PLR0913, PLR0917
    command: str,
    env: dict[str, Any] | None = None,
    helper_command: str | None = None,
    shell: str = "bash",
    return_all: bool = False,
    stream_level: int = logging.INFO,
    logger: logging.Logger | None = None,
    raise_on_failure: bool = True,
) -> list[str] | str:
    """Runs arbitrary shell commands as a util.

    Args:
        command: Shell command to be executed; can also be
            provided post-initialization by calling this task instance.
        env: Dictionary of environment variables to use for
            the subprocess; can also be provided at runtime.
        helper_command: String representing a shell command, which
            will be executed prior to the `command` in the same process.
            Can be used to change directories, define helper functions, etc.
            for different commands in a flow.
        shell: Shell to run the command with; defaults to "bash".
        return_all: Whether this task should return all lines of stdout as a list,
            or just the last line as a string; defaults to `False`.
        stream_level: The logging level of the stream.
        logger: Can pass a desired logger; if not passed, will automatically
            gets a run logger from Prefect.
        raise_on_failure: Whether to raise an exception if the command fails.

    Returns:
        If return all, returns all lines as a list; else the last line as a string.

    Example:
        Echo "hey it works".
        ```python
        from prefect_shell.utils import shell_run_command
        await shell_run_command("echo hey it works")
        ```
    """
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("prefect_shell.utils")

    current_env = os.environ.copy()
    current_env.update(env or {})

    with tempfile.NamedTemporaryFile(prefix="prefect-") as tmp:
        if helper_command:
            tmp.write(helper_command.encode())
            tmp.write(os.linesep.encode())
        tmp.write(command.encode())
        tmp.flush()

        shell_command = [shell, tmp.name]
        if sys.platform == "win32":
            shell_command = " ".join(shell_command)

        lines = []
        async with await open_process(shell_command, env=env) as process:
            async for text in TextReceiveStream(process.stdout):
                logger.log(stream_level, text)
                lines.extend(text.rstrip().split("\n"))

            await process.wait()
            if process.returncode:
                stderr = "\n".join(
                    [text async for text in TextReceiveStream(process.stderr)]
                )
                if not stderr and lines:
                    stderr = f"{lines[-1]}\n"
                msg = (
                    f"Command failed with exit code {process.returncode}:\n" f"{stderr}"
                )
                if raise_on_failure:
                    raise RuntimeError(msg)
                lines.append(msg)

    return lines if return_all else lines[-1]
