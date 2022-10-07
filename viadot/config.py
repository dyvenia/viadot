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
    DEFAULT_CONFIG = Config.from_yaml(
        join(USER_HOME, ".config", "viadot", "config.yaml"), key="sources"
    )
except FileNotFoundError:
    try:
        DEFAULT_CONFIG = Config.from_json(
            join(USER_HOME, ".config", "viadot", "config.json")
        )
    except FileNotFoundError:
        DEFAULT_CONFIG = Config()
