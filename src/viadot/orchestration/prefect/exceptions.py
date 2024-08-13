"""Exceptions to raise in case of issues while configuring or running viadot."""


class MissingSourceCredentialsError(Exception):
    """Raise when no source credentials were provided.

    Args:
        message (str, optional): A custom message to pass.
    """

    def __init__(self, message: str | None = None) -> None:
        """Override the default message."""
        default_message = "Either `credentials_secret`, `config_key`, or `credentials` has to be specified and not empty."
        message = message or default_message

        super().__init__(message)


class MissingPrefectBlockError(Exception):
    """Raise when a Prefect block is not found.

    Args:
        message (str, optional): A custom message to pass.
    """

    pass
