"""Viadot config."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from yaml import safe_load


logger = logging.getLogger(__name__)

USER_HOME = Path.home()
MAX_BATCH_SIZE = 20_000


class Config(dict):
    @classmethod
    def _get_configuration(cls, config: dict, key: str | None = None) -> dict | None:
        # Empty config.
        if not config:
            return None

        # Config key does not exist or has no values.
        if key:
            config = config.get(key)
            if not config:
                logger.warning(f"No configuration found under the '{key}' config key.")
                return None

        return cls(**config)

    @classmethod
    def from_yaml(cls, path: str | Path, key: str | None = None) -> Config:
        """Create a Config object from a YAML file.

        Args:
            path (str): The path to the YAML file.
            key (str | None, optional): The key inside the YAML. Defaults to None.

        Returns:
            Config: The Config object.
        """
        with Path(path).open() as f:
            config = safe_load(stream=f)
            return cls._get_configuration(config, key=key)


def _load_config(directory: Path) -> Config | None:
    """Load ``viadot.yaml`` from a directory if it exists."""
    path = directory / "viadot.yaml"
    if path.exists():
        try:
            return Config.from_yaml(path)
        except (TypeError, ValueError):
            logger.warning(f"Could not read viadot config file from '{path}'.")
            return Config()
    return None


def _find_project_config(start: Path | None = None) -> Config | None:
    """Find project configuration in the current or any parent directory.

    The closest configuration file wins, which makes nested projects predictable.
    """
    directory = (start or Path.cwd()).resolve()
    for candidate in (directory, *directory.parents):
        config = _load_config(candidate)
        if config is not None:
            return config
    return None


def _load_default_config() -> Config:
    """Load project config first, then fall back to the user's config."""
    project_config = _find_project_config()
    if project_config is not None:
        return project_config

    home_config = _load_config(Path(USER_HOME) / ".config" / "viadot")
    return home_config if home_config is not None else Config()


CONFIG = _load_default_config()


def get_max_batch_size(config: Config = CONFIG) -> int:
    """Return the configured maximum records per output batch."""
    settings = config.get("settings", {})
    value = (
        settings.get("max_batch_size", MAX_BATCH_SIZE)
        if isinstance(settings, dict)
        else MAX_BATCH_SIZE
    )
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        logger.warning(f"Invalid 'settings.max_batch_size'; using {MAX_BATCH_SIZE}.")
        return MAX_BATCH_SIZE
    return value


def get_source_config(key: str, config: Config = CONFIG) -> dict[str, Any] | None:
    """Retrieve source configuration.

    Args:
        key (str): The key inside the config to look for.
        config (Config, optional): The config object to extract from. Defaults to
            CONFIG.

    Returns:
        dict[str, Any]: Source configuration.
    """
    source_configs = config.get("sources")
    if source_configs is not None:
        for source_config in source_configs:
            if key in source_config:
                return source_config[key]
    return None


def get_source_credentials(key: str, config: Config = CONFIG) -> dict[str, Any] | None:
    """Retrieve source credentials from the provided config.

    Args:
        key (str): The key inside the config to look for.
        config (Config, optional): The config object to extract from. Defaults to
            CONFIG.

    Returns:
        dict[str, Any]: Source credentials.
    """
    source_config = get_source_config(key, config)
    if source_config is not None:
        return source_config.get("credentials")
    return None
