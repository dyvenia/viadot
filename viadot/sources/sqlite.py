from .base import SQL


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
