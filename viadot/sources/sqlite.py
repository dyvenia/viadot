from typing import Any, Dict, List, Literal

import pyodbc
from prefect.utilities import logging

from ..config import local_config
from .base import SQL, Source

logger = logging.get_logger(__name__)


class SQLite(SQL):
    """A SQLite source

    Args:
        server ([str]): server string, usually localhost
        db ([str]): the file path to the db e.g. /home/somedb.sqlite
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
