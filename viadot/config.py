from __future__ import annotations

import json
from os.path import expanduser, join
from typing import Optional

from yaml import safe_load

USER_HOME = expanduser("~")


class Config(dict):
    @classmethod
    def from_json(cls, path: str, key: Optional[str] = None) -> Config:
        with open(path) as f:
            config = json.load(f)
            if key:
                config = config[key]
            return cls(**config)

    @classmethod
    def from_yaml(cls, path: str, key: Optional[str] = None) -> Config:
        with open(path) as f:
            config = safe_load(stream=f)
            if key:
                config = config[key]
            return cls(**config)


try:
    CONFIG = Config.from_yaml(join(USER_HOME, ".config", "viadot", "config.yaml"))
except FileNotFoundError:
    try:
        CONFIG = Config.from_json(join(USER_HOME, ".config", "viadot", "config.json"))
    except FileNotFoundError:
        CONFIG = Config()


def get_source_config(key, config=CONFIG):
    source_configs = config.get("sources")
    if source_configs is not None:
        for source_config in source_configs:
            if key in source_config.keys():
                return source_configs[source_configs.index(source_config)][key]


def get_source_credentials(key, config=CONFIG):
    config = get_source_config(key, config)
    if config is not None:
        return config.get("credentials")
