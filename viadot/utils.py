from .exceptions import APIError
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from urllib3.exceptions import ProtocolError
from requests.packages.urllib3.util.retry import Retry
from typing import Any, Dict
import requests
from viadot.config import local_config
import pyodbc
import pandas as pd
from typing import List, Literal
from prefect.tasks.secrets import PrefectSecret
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret
import json
from viadot.sources import SQL


def slugify(name: str) -> str:
    return name.replace(" ", "_").lower()


def handle_api_response(
    url: str,
    auth: tuple = None,
    params: Dict[str, Any] = None,
    headers: Dict[str, Any] = None,
    timeout: tuple = (3.05, 60 * 30),
) -> requests.models.Response:
    """Handle and raise Python exceptions during request with retry strategy for specyfic status.

    Args:
        url (str): the URL which trying to connect.
        auth (tuple, optional): authorization information. Defaults to None.
        params (Dict[str, Any], optional): the request params also includes parameters such as the content type. Defaults to None.
        headers: (Dict[str, Any], optional): the request headers. Defaults to None.
        timeout (tuple, optional): the request times out. Defaults to (3.05, 60 * 30).

    Raises:
        ReadTimeout: stop waiting for a response after a given number of seconds with the timeout parameter.
        HTTPError: exception that indicates when HTTP status codes returned values different than 200.
        ConnectionError: exception that indicates when client is unable to connect to the server.
        APIError: defined by user.

    Returns:
        requests.models.Response
    """
    try:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        session.mount("http://", adapter)
        session.mount("https://", adapter)
        response = session.get(
            url,
            auth=auth,
            params=params,
            headers=headers,
            timeout=timeout,
        )

        response.raise_for_status()

    except ReadTimeout as e:
        msg = "The connection was successful, "
        msg += f"however the API call to {url} timed out after {timeout[1]}s "
        msg += "while waiting for the server to return data."
        raise APIError(msg)
    except HTTPError as e:
        raise APIError(
            f"The API call to {url} failed. "
            "Perhaps your account credentials need to be refreshed?",
        ) from e
    except (ConnectionError, Timeout) as e:
        raise APIError(f"The API call to {url} failed due to connection issues.") from e
    except ProtocolError as e:
        raise APIError(f"Did not receive any reponse for the API call to {url}.")
    except Exception as e:
        raise APIError("Unknown error.") from e

    return response


def generate_table_dtypes(
    credentials: str = None,
    table_name: str = None,
    db_name: str = None,
    reserve: float = 1.4,
    driver: str = None,
    only_dict: bool = True,
) -> dict:
    """Functon that automaticy generate dtypes dict from SQL table.

    Args:
        credentials (str, optional): Local credentials. Defaults to None.
        table_name (str): Table name. Defaults to None.
        db_name (str): Data base name. Defaults to None.
        reserve (str): How many signs add to varchar, percentage of value. Defaults to 1.4.
        driver (str): Pyodbc database driver. Defaults to None.
        only_dict (bool): choose to generate dictionary or whole dataframe. Defaults to True.

    Returns:
        Dictionary
    """
    sql = SQL(credentials=credentials, driver=driver)
    if db_name:
        sql.credentials["db_name"] = db_name

    sql.credentials["driver"] = driver

    query_admin = f"""select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
        NUMERIC_PRECISION, DATETIME_PRECISION, 
        IS_NULLABLE 
        from INFORMATION_SCHEMA.COLUMNS
        where TABLE_NAME='{table_name}'
        order by CHARACTER_MAXIMUM_LENGTH desc"""

    data = sql.run(query_admin)
    df = pd.DataFrame.from_records(data)
    create_int = lambda x: int(int(x) * reserve / 10) * 10 if int(x) > 30 else 30

    df[2] = df[2].astype(str).apply(lambda x: str(x.replace(".0", "")))
    df[2] = df[2].apply(
        lambda x: f"varchar({create_int(x)})" if x.isdigit() else "varchar(500)"
    )
    if only_dict:
        return dict(zip(df[0], df[2]))
    else:
        return df
