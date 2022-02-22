from viadot.sources.base import SQL
from viadot.exceptions import CredentialError
from typing import Any, Dict, List
from viadot.config import local_config
import pandas as pd
from prefect import task


@task
def query_result_to_df_task(result: List[tuple], cols: List[str]):
    return pd.DataFrame.from_records(result, columns=cols)


class ASELite(SQL):
    """
    Python class that inheridate form SQL class located in viadot platform.

    Args:
        credentials: (Dict[str, Any] , optional): Also credentials can be stored in credantials.json file in config folder.
        It can be necessary to change driver type: "driver": "PostgreSQL Unicode"
    """

    def __init__(
        self, credentials: Dict[str, Any] = None, db_name: str = None, *args, **kwargs
    ):
        DEFAULT_CREDENTIALS = local_config.get("ASLite_SQL")
        credentials = DEFAULT_CREDENTIALS or credentials
        if credentials is None:
            raise CredentialError("Missing credentials.")

        super().__init__(*args, credentials=credentials, **kwargs)
        self.credentials = credentials
        self.credentials["db_name"] = db_name

    def to_df(self, query: str, if_empty: str = None) -> pd.DataFrame:
        """Creates DataFrame form SQL query.
        Args:
            query (str): SQL query. If don't start with "SELECT" returns empty DataFrame.
            if_empty (str, optional): What to do if the query returns no data. Defaults to None.
        """
        conn = self.con
        if query.upper().startswith("SELECT"):
            df = pd.read_sql_query(query, conn)
            if df.empty:
                self._handle_if_empty(if_empty=if_empty)
        else:
            df = pd.DataFrame()
        return df
