from typing import Any, Dict

import pendulum
import prefect
import requests
from prefect.utilities.graphql import EnumValue, with_args
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from requests.packages.urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError

from .exceptions import APIError


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


def get_flow_last_run_date(flow_name: str) -> str:
    """
    Retrieve a flow's last run date as an ISO datetime string.

    This function assumes you are already authenticated with Prefect Cloud.
    """
    client = prefect.Client()
    result = client.graphql(
        {
            "query": {
                with_args(
                    "flow_run",
                    {
                        "where": {
                            "flow": {"name": {"_eq": flow_name}},
                            "start_time": {"_is_null": False},
                            "state": {"_eq": "Success"},
                        },
                        "order_by": {"start_time": EnumValue("desc")},
                        "limit": 1,
                    },
                ): {"start_time"}
            }
        }
    )
    flow_run_data = result.get("data", {}).get("flow_run")

    if not flow_run_data:
        return None

    last_run_date_raw_format = flow_run_data[0]["start_time"]
    last_run_date = (
        pendulum.parse(last_run_date_raw_format).format("YYYY-MM-DDTHH:MM:SS") + "Z"
    )
    return last_run_date
