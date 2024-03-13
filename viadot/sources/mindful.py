import os
import json
import logging
from io import StringIO
from pydantic import BaseModel
from requests.models import Response
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple, Literal, Optional


import pandas as pd

from ..config import get_source_credentials
from ..exceptions import CredentialError, APIError
from ..utils import handle_api_response
from .base import Source

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MINDFUL_CREDENTIALS(BaseModel):
    """Checking for values in Mindful credentials dictionary.
    In mindful there are two key values:
        - customer_uuid: The unique ID for the organization.
        - auth_token: A unique token to be used as the password for API requests.

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating Pydantic models.
    """

    customer_uuid: str
    auth_token: str


class Mindful(Source):
    """
    Class implementing the mindful API.
    Documentation for this API is located at: https://apidocs.surveydynamix.com/.
    """

    ENDPOINTS = ["interactions", "responses", "surveys"]
    key_credentials = ["customer_uuid", "auth_token"]

    def __init__(
        self,
        region: Literal["us1", "us2", "us3", "ca1", "eu1", "au1"] = "eu1",
        credentials: MINDFUL_CREDENTIALS = None,
        config_key: str = "mindful",
        *args,
        **kwargs,
    ):
        """
        Description:
            Create an instance of mindful.
        Args:
            region (Literal[us1, us2, us3, ca1, eu1, au1], optional): SD region from where to interact with the mindful API.
                Defaults to "eu1".
            credentials (MINDFUL_CREDENTIALS): Credentials to mindful. Defaults to None.
            config_key (str, optional): The key in the viadot config holding relevant credentials. Defaults to "mindful".
        """

        credentials = credentials or get_source_credentials(config_key) or {}
        if credentials is None:
            raise CredentialError("Missing credentials.")

        logging.basicConfig()
        validated_creds = dict(MINDFUL_CREDENTIALS(**credentials))
        super().__init__(*args, credentials=validated_creds, **kwargs)

        self.auth = (credentials["customer_uuid"], credentials["auth_token"])
        # self.header = {"Authorization": f"Bearer {credentials.get('vault')}"}
        if region != "us1":
            self.region = region + "."
        else:
            self.region = ""

    def _check_timestamp(
        self,
        start_date: datetime,
        end_date: datetime,
        date_interval: int,
    ) -> Tuple[datetime]:
        """Function to prove the correctness of the date interval.

        Args:
            start_date (datetime, optional): Start date of the request. Defaults to None.
            end_date (datetime, optional): End date of the resquest. Defaults to None.
            date_interval (int, optional): How many days are included in the request.
                If end_date is passed as an argument, date_interval will be invalidated. Defaults to 1.

        Raises:
            ValueError: Exception raised if `start_date` > `end_date`.

        Returns:
            Tuple[datetime]: `start_date` and `end_date` once proven inconsistencies are done.
        """
        if not isinstance(start_date, datetime):
            start_date = datetime.now() - timedelta(days=date_interval)
            end_date = start_date + timedelta(days=date_interval)
            self.logger.warning(
                f"Mindful 'start_date' and 'end_date' parameter are None or not in datetime format, redefined to:\n\t\t{start_date}\n\t\t{end_date}"
            )
        elif isinstance(start_date, datetime) and not isinstance(end_date, datetime):
            end_date = start_date + timedelta(days=date_interval)
            if end_date > datetime.now():
                end_date = datetime.now()
            print(
                f"Mindful 'end_date' variable is None or not in datetime format, it has been taken as: {self.end_date}."
            )
        elif start_date >= end_date:
            raise ValueError(
                "'start_date' parameter must be lower than 'end_date' variable."
            )
        else:
            print(
                f"Mindful files to download will store data from {start_date} to {end_date}."
            )

        return start_date, end_date

    def _mindful_api_response(
        self,
        params: Dict[str, Any] = None,
        endpoint: str = "",
        **kwargs,
    ) -> Response:
        """Basic call to Mindful API given an endpoint.

        Args:
            params (Dict[str, Any], optional): Parameters to be passed into the request. Defaults to None.
            endpoint (str, optional): API endpoint for an individual request. Defaults to "".

        Returns:
            Response: request object with the response from the Mindful API.
        """

        response = handle_api_response(
            url=f"https://{self.region}surveydynamix.com/api/{endpoint}",
            params=params,
            method="GET",
            auth=HTTPBasicAuth(*self.auth),
        )

        return response

    def to_df(
        self,
        endpoint: str = "",
        start_date: datetime = None,
        end_date: datetime = None,
        date_interval: int = 1,
        limit: int = 1000,
        **kwargs,
    ) -> Response:
        """Gets a list of survey interactions as a JSON array of interaction resources.

        Args:
            endpoint (str, optional): API endpoint for an individual request. Defaults to ".
            start_date (datetime, optional): Start date of the request. Defaults to None.
            end_date (datetime, optional): End date of the resquest. Defaults to None.
            date_interval (int, optional): How many days are included in the request.
                If end_date is passed as an argument, date_interval will be invalidated. Defaults to 1.
            limit (int, optional): The number of matching interactions to return. Defaults to 1000.

        Raises:
            ValueError: Not available endpoint.
            APIError: Failed to download data from endpoint.

        Returns:
            Response: request object with the response from the Mindful API.
        """
        if endpoint not in self.ENDPOINTS:
            raise ValueError(
                f"endpoint {endpoint} not available through viadot connection, yet."
            )

        params = {
            "_limit": limit,
            "start_date": f"{start_date}",
            "end_date": f"{end_date}",
        }

        if endpoint == "surveys":
            del params["start_date"]
            del params["end_date"]
        else:
            start_date, end_date = self._check_timestamp(
                start_date, end_date, date_interval
            )
            params["start_date"] = start_date
            params["end_date"] = end_date

        response = self._mindful_api_response(
            endpoint=endpoint,
            params=params,
        )

        if response.status_code == 200:
            print(f"Succesfully downloaded {endpoint} data from mindful API.")
            data = StringIO(response.content.decode("utf-8"))
        elif response.status_code == 204 and not response.content.decode():
            print(
                f"WARNING: Thera are not {endpoint} data to download from {start_date or ' --- '} to {end_date  or ' --- '}."
            )
            data = json.dumps({})
        else:
            print(f"ERROR: Failed to downloaded {endpoint} data. - {response.content}")
            raise APIError(f"Failed to downloaded {endpoint} data.")

        data_frame = pd.read_json(data)
        if data_frame.empty:
            self._handle_if_empty(
                if_empty="warn",
                message=f"No data retrieved from the endpoint: {endpoint}, days from {start_date} to {end_date}",
            )

        return data_frame

    def to_file(
        self,
        data_frame: pd.DataFrame,
        file_path: str = "",
        file_name: Optional[str] = None,
        file_extension: Literal["parquet", "csv"] = "csv",
        sep: str = "\t",
    ) -> str:
        """Save Mindful response data to file and return filename.

        Args:
            data_frame (pd.DataFrame): Reference data-frame to be saved into a file.
            file_path (str, optional): Path where to save the file locally. Defaults to ''.
            file_name (str, optional): Name of the file without extension. Defaults to None.
            file_extension (Literal[parquet, csv], optional): File extensions for storing responses. Defaults to "csv".
            sep (str, optional): Separator in csv file. Defaults to "\t".

        Example:
            surveys_file_name = mindful.to_file(
                data_frame,
                file_path="your/local/path",
                file_name="interactions",
                file_extension="csv",
                sep="\t",
            )

        returns
            str: The absolute path of the downloaded file.
        """

        if file_name is None:
            complete_file_name = f"mindful_response_{datetime.now().strftime('%Y%m%d%H%M%S')}.{file_extension}"
            relative_path = os.path.join(file_path, complete_file_name)
        else:
            complete_file_name = f"{file_name}.{file_extension}"
            relative_path = os.path.join(file_path, complete_file_name)

        if file_extension == "csv":
            data_frame.to_csv(relative_path, index=False, sep=sep)
        elif file_extension == "parquet":
            data_frame.to_parquet(relative_path, index=False)
        else:
            raise ValueError("`file_extension` must be one of: 'csv', 'parquet'.")

        return relative_path
