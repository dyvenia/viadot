import json
from os.path import expanduser, join

USER_HOME = expanduser("~")


class Config(dict):
    @classmethod
    def from_json(cls, path: str, key: str = None):
        with open(path) as f:
            config = json.load(f)
            if key:
                config = config[key]
            return cls(**config)


try:
    local_config = Config.from_json(join(USER_HOME, ".config", "credentials.json"))
except FileNotFoundError:
    local_config = None
