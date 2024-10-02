"""Viadot config."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from yaml import safe_load


logger = logging.getLogger(__name__)

USER_HOME = Path.home()


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
    def from_json(cls, path: str | Path, key: str | None = None) -> Config:
        """Create a Config object from a JSON file.

        Args:
            path (str): The path to the JSON file.
            key (str | None, optional): The key inside the JSON. Defaults to None.

        Returns:
            Config: The Config object.
        """
        with Path(path).open() as f:
            config = json.load(f)
            return cls._get_configuration(config, key=key)

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


config_dir = Path(USER_HOME) / ".config" / "viadot"

try:
    CONFIG = Config.from_yaml(config_dir / "config.yaml")
except FileNotFoundError:
    try:
        CONFIG = Config.from_json(config_dir / "config.json")
    except FileNotFoundError:
        CONFIG = Config()
except ValueError:
    # Incorrect config structure.
    logger.warning(f"Could not read default viadot config file from '{config_dir}'.")
    CONFIG = Config()


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
