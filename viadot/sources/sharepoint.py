from ..config import local_config
from ..exceptions import CredentialError
from .base import Source
from viadot.utils import get_nested_dict

from typing import Any, Dict, List
from fnmatch import fnmatch
from datetime import datetime
from copy import deepcopy
import pandas as pd

import sharepy
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.runtime.client_request_exception import ClientRequestException
from prefect.utilities import logging


logger = logging.get_logger()

# Print out how many rows was extracted in specific iteration
def log_of_progress(items):
    logger.info("Items read: {0}".format(len(items)))


class Sharepoint(Source):
    """
    A Sharepoint class to connect and download specific Excel file from Sharepoint.

    Args:
        credentials (dict): In credentials should be included:
            "site" - Path to sharepoint website (e.g : {tenant_name}.sharepoint.com)
            "username" - Sharepoint username (e.g username@{tenant_name}.com)
            "password"
        download_from_path (str, optional): Full url to file
                        (e.g : https://{tenant_name}.sharepoint.com/sites/{directory}/Shared%20Documents/Dashboard/file). Defaults to None.
    """

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        download_from_path: str = None,
        *args,
        **kwargs,
    ):
        DEFAULT_CREDENTIALS = local_config.get("SHAREPOINT")
        credentials = credentials or DEFAULT_CREDENTIALS
        if credentials is None:
            raise CredentialError("Credentials not found.")
        self.url = download_from_path
        self.required_credentials = ["site", "username", "password"]
        super().__init__(*args, credentials=credentials, **kwargs)

    def get_connection(self) -> sharepy.session.SharePointSession:
        if any([rq not in self.credentials for rq in self.required_credentials]):
            raise CredentialError("Missing credentials.")

        return sharepy.connect(
            site=self.credentials["site"],
            username=self.credentials["username"],
            password=self.credentials["password"],
        )

    def download_file(
        self,
        download_from_path: str = None,
        download_to_path: str = "Sharepoint_file.xlsm",
    ) -> None:
        """Function to download files from Sharepoint.
        Args:
            download_from_path (str): Path from which to download file. Defaults to None.
            download_to_path (str, optional): Path to destination file. Defaults to "Sharepoint_file.xlsm".
        """
        download_from_path = download_from_path or self.url
        if not download_from_path:
            raise ValueError("Missing required parameter 'download_from_path'.")

        conn = self.get_connection()
        conn.getfile(
            url=download_from_path,
            filename=download_to_path,
        )


class SharepointList(Source):
    """
    A Sharepoint_List class to connect and download data from sharpoint lists.

    Args:
        credentials (dict): Credentials should include:
           - "tenant"
           - "client_id"
           - "scopes"
           - "thumbprint"
           - "private_key"
    """

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):
        DEFAULT_CREDENTIALS = local_config.get("SHAREPOINT_CERT")
        credentials = credentials or DEFAULT_CREDENTIALS
        if credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

    def get_connection(
        self,
        site_url: str = None,
    ):

        # Connecting into Sharepoint with AuthenticationContext
        try:
            auth_context = AuthenticationContext(site_url)
            auth_context.with_client_certificate(
                tenant=self.credentials["TENANT"],
                client_id=self.credentials["CLIENT_ID"],
                scopes=[self.credentials["SCOPES"]],
                thumbprint=self.credentials["THUMBPRINT"],
                private_key=self.credentials["PRIVATE_KEY"],
            )

            self.ctx = ClientContext(site_url, auth_context)
            logger.info("Successfully connect to Sharepoint Lists")

        except Exception as ex:
            logger.info(f"Error at ClientContext or authentication level: {ex}")

        return self.ctx

    # Function for extracting list items from search fields
    def _unpack_fields(
        self,
        list_item,
        selected_fields: dict = None,
    ):

        # Creating the body of dictionary
        new_dict = dict()

        # For loop scanning the propertys of searching fields
        item_values_dict = list_item.properties
        for field, val in item_values_dict.items():
            nested_dict = get_nested_dict(val)
            # Check if the dictionary is nested
            if nested_dict != None:
                # It might be that there are different field properties than expected
                nested_value = nested_dict.get(selected_fields["FieldProperty"])
                if nested_value != None:
                    new_dict[field] = nested_value
                else:
                    logger.info("I'm not the right value")
                    raise ValueError
            else:
                new_dict[field] = val

        return new_dict

    def get_fields(
        self,
        list_title: str = None,
        site_url: str = None,
        required_fields: List[str] = None,
    ):

        ctx = self.get_connection(site_url=site_url)

        # Get list of lists object by List Title
        self.list_object = ctx.web.lists.get_by_title(list_title)
        list_fields_all = self.list_object.fields

        # Get all or specifics list of objects
        if required_fields is None:
            ctx.load(list_fields_all)
            ctx.execute_query()

            return list_fields_all

        else:
            list_fields_required = [
                list_fields_all.get_by_internal_name_or_title(field).get()
                for field in required_fields
            ]
            ctx.execute_batch()

            return list_fields_required

    def select_expandable_user_fields(
        self,
        list_title: str = None,
        site_url: str = None,
        required_fields: List[str] = None,
        field_property: str = "Title",
    ):
        """
        Method to expand fields and get more informations.
        field_property to expand can be: ID, Title, FieldTypeKind, TypeAsString and many more.
            -> more properties can be discovered by getting list.item.properties.
            Default to "Title"
        """

        list_fields = self.get_fields(
            list_title=list_title, site_url=site_url, required_fields=required_fields
        )

        # Finding the "selected" fields
        fields_to_select = [
            field.properties["InternalName"] + f"/{field_property}"
            if fnmatch(field.properties["TypeAsString"], "User*")
            else field.properties["InternalName"]
            for field in list_fields
        ]
        # Finding the "expanded" fields
        fields_to_expand = [
            field.properties["InternalName"]
            for field in list_fields
            if fnmatch(field.properties["TypeAsString"], f"User*")
        ]

        # Creating the body of the function output
        selected_fields = {
            "FieldInternalNames": fields_to_select,
            "FieldToExpand": fields_to_expand,
            "FieldProperty": field_property,
        }

        return selected_fields

    def check_filters(
        self,
        filters: dict = None,
    ) -> bool:
        """
        Function to check if filters dict is valid.
            example1: if operator2 is present value2 must be in place as well
            example2: if dtype is not on allowed list it will throw an error
        """

        allowed_dtypes = ["datetime", "date", "bool", "int", "float", "complex", "str"]
        allowed_conjuction = ["&", "|"]
        allowed_operators = ["<", ">", "<=", ">=", "==", "!="]

        for parameters in filters.values():
            if parameters.get("dtype") not in allowed_dtypes:
                raise ValueError(
                    f"dtype not allowed! Expected {allowed_dtypes} got: {parameters.get('dtype')}."
                )
            if parameters.get("operator1"):
                if parameters.get("operator1") not in allowed_operators:
                    raise ValueError(
                        f"Operator type not allowed! Expected {allowed_operators} got: {parameters.get('operator1')}."
                    )
                if not parameters.get("value1"):
                    raise ValueError("Value for operator1 is missing!")
            elif not parameters.get("operator1"):
                raise ValueError("Operator1 is missing!")
            if (
                not parameters.get("operator2")
                and parameters.get("operators_conjuction") is not None
            ):
                raise ValueError(
                    f"Operator conjuction allowed only with more than one filter operator!"
                )
            if parameters.get("operator2"):
                if parameters.get("operator2") not in allowed_operators:
                    raise ValueError(
                        f"Operator type not allowed! Expected {allowed_operators} got: {parameters.get('operator2')}."
                    )
                if not parameters.get("value2"):
                    raise ValueError("Value for operator2 is missing!")
                if not parameters.get("operators_conjuction"):
                    raise ValueError(
                        f"Operators for conjuction is missing! Expected {allowed_conjuction} got empty."
                    )
                if parameters.get("operators_conjuction") not in allowed_conjuction:
                    raise ValueError(
                        f"Operators for conjuction not allowed! Expected {allowed_conjuction} got {parameters.get('operators_conjuction')}."
                    )
            if parameters.get("filters_conjuction"):
                if (
                    len(filters.keys()) == 1
                    and parameters.get("filters_conjuction") is not None
                ):
                    raise ValueError(
                        f"Filters conjuction allowed only with more than one filter column!"
                    )
                if parameters.get("filters_conjuction") not in allowed_conjuction:
                    raise ValueError(
                        f"Filters operators for conjuction not allowed! Expected {allowed_conjuction} got {parameters.get('filters_conjuction')}."
                    )

        return True

    def operators_mapping(
        self,
        filters: dict = None,
    ) -> dict:
        """
        Function for mapping comparison and conjuction(logical) operators of filters to the format which is recognized by Microsoft API.

        Args:
            filters (dict): A dictionar which contains operators.

        Returns:
            New modified dict.
        """

        filters_dict = deepcopy(filters)
        operators = {
            "<": "lt",
            ">": "gt",
            "<=": "le",
            ">=": "ge",
            "==": "eq",
            "!=": "ne",
        }
        logical_op = {"&": "and", "|": "or"}

        for parameters in filters_dict.values():
            if parameters.get("operator1"):
                operator1_to_change = parameters.get("operator1")
                if operator1_to_change in operators.keys():
                    parameters["operator1"] = operators[operator1_to_change]
                else:
                    raise ValueError(
                        f"This comparison operator: {operator1_to_change} is not allowed. Please read the function documentation for details!"
                    )
            if parameters.get("operator2"):
                operator2_to_change = parameters.get("operator2")
                if operator2_to_change in operators.keys():
                    parameters["operator2"] = operators[operator2_to_change]
                else:
                    raise ValueError(
                        f"This comparison operator: {operator2_to_change} is not allowed. Please read the function documentation for details!"
                    )
            if parameters.get("operators_conjuction"):
                logical_op_to_change = parameters.get("operators_conjuction")
                if logical_op_to_change in logical_op.keys():
                    parameters["operators_conjuction"] = logical_op[
                        logical_op_to_change
                    ]
                else:
                    raise ValueError(
                        f"This conjuction(logical) operator: {logical_op_to_change} is not allowed. Please read the function documentation for details!"
                    )
            if parameters.get("filters_conjuction"):
                logical_fl_to_change = parameters.get("filters_conjuction")
                if logical_fl_to_change in logical_op.keys():
                    parameters["filters_conjuction"] = logical_op[logical_fl_to_change]
                else:
                    raise ValueError(
                        f"This conjuction(logical) operator: {logical_fl_to_change} is not allowed. Please read the function documentation for details!"
                    )

        return filters_dict

    def make_filter_for_api(self, filters: dict) -> "str":
        """
        Function changing type of operators to match MS API style as 'str' passing to URL call.

        Args:
            filters (dict): A dictionar which contains operators.

        Returns:
            Output as string to pass as filter parameter to API.
        """

        filter_text = ""
        filters_mod = self.operators_mapping(filters)

        for column, parameters in filters_mod.items():
            if parameters.get("dtype") in ["datetime", "date"]:
                from_date1 = datetime.strptime(
                    parameters.get("value1"), "%Y-%m-%d"
                ).isoformat()
                filter_text = (
                    filter_text
                    + f"{column} {parameters.get('operator1')} datetime'{from_date1}' "
                )
                if parameters.get("operator2"):
                    from_date2 = datetime.strptime(
                        parameters.get("value2"), "%Y-%m-%d"
                    ).isoformat()
                    filter_text = (
                        filter_text
                        + f" {parameters.get('operators_conjuction')} {column} {parameters.get('operator2')} datetime'{from_date2}' "
                    )
            elif parameters.get("dtype") not in ["datetime", "date"]:
                filter_text = (
                    filter_text
                    + f"{column} {parameters.get('operator1')} '{parameters.get('value1')}'"
                )
                if parameters.get("operator2"):
                    filter_text = (
                        filter_text
                        + f"{column} {parameters.get('operator2')} '{parameters.get('value2')}'"
                    )
            if parameters.get("filters_conjuction"):
                filter_text = filter_text + f"{parameters.get('filters_conjuction')} "

        return filter_text

    def make_filter_for_df(
        self,
        filters: dict = None,
    ) -> "str":
        """
        Function changing dict operators into pandas DataFrame filters.

        Args:
            filters (dict): A dictionar which contains operators.

        Returns:
            Output as string to pass as filter to DataFrame.
        """

        filter_in_df = "df.loc["

        for column, parameters in filters.items():
            filter_in_df = (
                filter_in_df
                + f"(df.{column} {parameters.get('operator1', '')} '{parameters.get('value1', '')}'"
            )

            if parameters.get("operator2"):
                filter_in_df = (
                    filter_in_df
                    + f") {parameters.get('operators_conjuction')} (df.{column} {parameters.get('operator2', '')} '{parameters.get('value2', '')}'"
                )

            if parameters.get("filters_conjuction"):
                filter_in_df = filter_in_df + ")" + parameters.get("filters_conjuction")

            else:
                filter_in_df = filter_in_df + ")"

        filter_in_df = filter_in_df + "]"

        return filter_in_df

    def list_item_to_df(
        self,
        list_title: str = None,
        site_url: str = None,
        required_fields: List[str] = None,
        field_property: str = "Title",
        filters: dict = None,
        row_count: int = 5000,
    ):
        """
        Method to extract data from Sharepoint List into DataFrame.

        Args:
            list_title (str): Title of Sharepoint List. Default to None.
            site_url (str): URL to set of Sharepoint Lists. Default to None.
            required_fields (List[str]): Required fields(columns) need to be extracted from
                                         Sharepoint List. Default to None.
            field_property (List[str]): Property to expand with expand query method.
                                        All propertys can be found under list.item.properties.
                                        Default to ["Title"]
            filters (dict): Dictionary with operators which filters the SharepointList output.
                            allowed dtypes: ('datetime','date','bool','int', 'float', 'complex', 'str')
                            allowed conjuction: ('&','|')
                            allowed operators: ('<','>','<=','>=','==','!=')
                            Example how to build the dict:
                            filters = {
                            'Column_name_1' :
                                    {
                                    'dtype': 'datetime',
                                    'value1':'YYYY-MM-DD',
                                    'value2':'YYYY-MM-DD',
                                    'operator1':'>=',
                                    'operator2':'<=',
                                    'operators_conjuction':'&',
                                    'filters_conjuction':'&',
                                    }
                                    ,
                            'Column_name_2' :
                                    {
                                    'dtype': 'str',
                                    'value1':'NM-PL',
                                    'operator1':'==',
                                    },
                            }
            row_count (int): Number of downloaded rows in single request. Default to 5000.

        Returns:
            pd.DataFrame
        """

        # checking if the passed filters dictionary is correctly built
        if filters is not None:
            self.check_filters(filters)
            # checking that the filter parameters are included in the desired field parameters
            for key in filters:
                if key not in required_fields:
                    raise AttributeError(
                        f"Filter '{key}' not included inside required fields. It is obligatory to extract data which is filtered!"
                    )

            # changing the body of the filter for MS API call
            filter_text = self.make_filter_for_api(filters)

        download_all = False

        # extracting requeird_fields SP_List objects
        selected_fields = self.select_expandable_user_fields(
            list_title=list_title,
            site_url=site_url,
            required_fields=required_fields,
            field_property=field_property,
        )

        try:
            # Extract data below 5k rows or max limitation of the specific SP List with basic filtering.
            if filters is None:
                raise ValueError("There is no filter. Starting extraxction all data")
            else:
                list_items = (
                    self.list_object.items.filter(filter_text)
                    .select(selected_fields["FieldInternalNames"])
                    .top(row_count)
                    .expand(selected_fields["FieldToExpand"])
                )
            self.ctx.load(list_items)
            self.ctx.execute_query()

        except (ClientRequestException, ValueError) as e:
            # Extract all data from specific SP List without basic filtering. Additional logic for filtering applied on DataFreame level.
            logger.info(f"Exception SPQueryThrottledException occurred: {e}")
            list_items = (
                self.list_object.items.get_all(row_count, log_of_progress)
                .select(selected_fields["FieldInternalNames"])
                .expand(selected_fields["FieldToExpand"])
            )
            self.ctx.load(list_items)
            self.ctx.execute_query()
            download_all = True

        df = pd.DataFrame(
            [self._unpack_fields(row_item, selected_fields) for row_item in list_items]
        )

        if download_all == True and filters is not None:
            # Filter for desired range of created date  and for factory Namyslow PL
            self.logger.info("Filtering df with all data output")
            filter_for_df = self.make_filter_for_df(filters)
            df = eval(filter_for_df)

        return df
