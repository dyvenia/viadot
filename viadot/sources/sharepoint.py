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
    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):
        """
        A Sharepoint_List class to connect and download data from Sharepoint lists.
        Warning!
            Please be careful with selection of the column names because once sharepoint list is opened inside a browser it may display columns in different languages.
            Because of that the resulting file or output might have different column names then the one which u see in the browser.

        Args:
            credentials (Dict[str, Any], optional): Credentials should include:
           - "tenant"
           - "client_id"
           - "scopes"
           - "thumbprint"
           - "private_key"

        Raises:
            CredentialError: If no credentials are passed
            and local config doesn't contain them neither
        """
        DEFAULT_CREDENTIALS = local_config.get("SHAREPOINT_CERT")
        credentials = credentials or DEFAULT_CREDENTIALS
        if credentials is None:
            raise CredentialError("Credentials not found.")

        super().__init__(*args, credentials=credentials, **kwargs)

    def get_connection(self, site_url: str):
        """Function for connecting into Sharepoint with AuthenticationContext.

        Args:
            site_url (str): URL of the sharepoint list.

        Returns:
            ctx: Authentication context.
        """
        logger.info("Connecting into Sharepoint with AuthenticationContexts.")
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

    def _unpack_fields(
        self,
        list_item,
        selected_fields: dict,
    ) -> dict:
        """Function for extracting and unpacking list items from the search fields.

        Args:
            list_items (office365 list item): A list with office365 list item objects (rows)
            selected_fields (dict): A dict with fields selected for ingestion, generated by SharepointList.select_fields()

        Raises:
            ValueError: "Check if given field property is valid!"
            ValueError: "Get nested dict for not recognized type of field! Check field types in the source."
            ValueError: "Get empty properties for list items."

        Returns:
            dict: A dictionary with Column: Value pairs for each row from the list.
        """
        # Creating the body of dictionary
        new_dict = dict()
        # For loop scanning the properties of searching fields
        item_values_dict = list_item.properties
        if item_values_dict:
            for field, val in item_values_dict.items():
                nested_dict = get_nested_dict(val)
                # Check if the values are nested
                if nested_dict != None:
                    # Check if field has expandable type
                    if field in selected_fields["FieldToExpand"]:
                        # It might be that there are different field properties than expected
                        nested_value = nested_dict.get(
                            selected_fields["FieldExpandProperty"]
                        )
                        if nested_value != None:
                            new_dict[field] = nested_value
                        else:
                            raise ValueError("Check if given field property is valid!")
                    elif field in selected_fields["MultiChoiceField"]:
                        # Field type of multi choice could have more than 1 selection.
                        new_dict[field] = ";".join(nested_dict.values())
                    else:
                        raise ValueError(
                            "Get nested dict for not recognized type of field! Check field types in the source."
                        )
                else:
                    new_dict[field] = val
        else:
            raise ValueError(
                "Get empty properties for list items. Check if parameter list_item collection contains any data -> item objects."
            )
        return new_dict

    def get_fields(
        self,
        list_title: str,
        site_url: str,
        required_fields: List[str] = None,
    ) -> List:
        """
        Function for getting list of fields objects from the sharepoint list.
        It can get all fields available if required_fields not passed
        or just the one which are in the list required_fields.

        Args:
            list_title (str): Name of the sharepoint list.
            site_url (str): URL to the sharepoint list with "/" at the end.
            required_fields (List[str], optional ): List of required fields to ingest. It will get all fields if not passed.

        Returns:
            List: List with office365 sharepoint list field objects.
        """

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
                list_fields_all.get_by_internal_name_or_title(field)
                .get()
                .execute_query()
                for field in required_fields
            ]

            return list_fields_required

    def select_fields(
        self,
        list_title: str,
        site_url: str,
        required_fields: List[str] = None,
        field_property: str = "Title",
    ) -> dict:
        """
        Method to create a data structure for handling info about
        selection of fields with details about possible expansion for more data or details.
        Field types to extract more values can be: "User*", "MultiChoice"
        field_property to expand can be: ID, Title, FieldTypeKind, TypeAsString and many more.
            -> more properties can be discovered by getting list.item.properties.

        Args:
            list_title (str): A title of the sharepoint list. Defaults to None.
            site_url (str): A sharepoint list URL. Defaults to None.
            required_fields (List[str], optional): List of fields(columns) to be ingested. Defaults to None.
            field_property (str, optional): Property to extract from nested fields
                like column with type User*. Defaults to "Title".

        Returns:
            dict:  selected_fields = {
                    "FieldInternalNames": List of fields to select with its InternalNames (from api),
                    "FieldToExpand": fields_to_expand,-> fields which could be expanded to get more data from API
                    "FieldExpandProperty": field_property, property of the expandable field which will be extracted
                    "MultiChoiceField": List of fields which can have multiple values in 1 row
        }
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
        multi_choice_fields = [
            field.properties["InternalName"]
            for field in list_fields
            if fnmatch(field.properties["TypeAsString"], "MultiChoice")
        ]
        # Creating the body of the function output
        selected_fields = {
            "FieldInternalNames": fields_to_select,
            "FieldToExpand": fields_to_expand,
            "FieldExpandProperty": field_property,
            "MultiChoiceField": multi_choice_fields,
        }

        return selected_fields

    def check_filters(
        self,
        filters: dict,
    ) -> bool:
        """
        Function to check if filters dict is valid.
        Please check and apply only allowed filter settings:
            allowed_dtypes = ["datetime", "date", "bool", "int", "float", "complex", "str"]
            allowed_conjunction = ["&", "|"]
            allowed_operators = ["<", ">", "<=", ">=", "==", "!="]
        Operator conjunction is only possible if there are 2 values like: value <= 1 | value == 5
        Filter conjunction is only possible if there are more then 1 filters for ex. date and creator

        Args:
            filters (dict): A dictionary containing filter settings
                Example:
                        filters = {
                                    "Created": {
                                            "dtype": "datetime",
                                            "value1": yesterday_date,
                                            "value2": today_date,
                                            "operator1": ">=",
                                            "operator2": "<=",
                                            "operators_conjunction": "&",
                                            "filters_conjunction": "&",
                                            },
                                    "Factory": {
                                        "dtype": "str",
                                        "value1": "NM-PL",
                                        "operator1": "==",
                                        },
                                    }

        Raises:
            ValueError: If dtype not in allowed list.
            ValueError: If comparison operator1 not in allowed list.
            ValueError: If value for operator1 is missing.
            ValueError: If comparison operator1 for the first value is missing.
            ValueError: If comparison operator2 not in allowed list.
            ValueError: If value for operator2 is missing.
            ValueError: If comparison operator2 for the first value is missing.
            ValueError: If operator conjunction is missing while there are 2 values and 2 operators passed.
            ValueError: If operator conjunction is not in the allowed list.
            ValueError: If operator conjunction provided why only one filter value is given.
            ValueError: If filter conjunction provided without 2nd filter.
            ValueError: If filter conjunction not in the allowed list.

        Returns:
            bool: True if all checks passed.
        """

        allowed_dtypes = ["datetime", "date", "bool", "int", "float", "complex", "str"]
        allowed_conjunction = ["&", "|"]
        allowed_operators = ["<", ">", "<=", ">=", "==", "!="]

        for filter_name, parameters in filters.items():
            if not parameters.get("dtype"):
                raise ValueError(f"dtype for {filter_name} is missing!")
            if parameters.get("dtype") not in allowed_dtypes:
                raise ValueError(
                    f"dtype not allowed! Expected: {allowed_dtypes} got: {parameters.get('dtype')} ."
                )
            if parameters.get("operator1"):
                if parameters.get("operator1") not in allowed_operators:
                    raise ValueError(
                        f"Operator1 type not allowed! Expected: {allowed_operators} got: {parameters.get('operator1')} ."
                    )
                if not parameters.get("value1"):
                    raise ValueError("Value1 for operator1 is missing!")
            elif not parameters.get("operator1"):
                raise ValueError("Operator1 is missing!")
            if (
                not parameters.get("operator2")
                and parameters.get("operators_conjunction") is not None
            ):
                raise ValueError(
                    f"Operator conjunction allowed only with more then one filter operator!"
                )
            if parameters.get("operator2"):
                if parameters.get("operator2") not in allowed_operators:
                    raise ValueError(
                        f"Operator2 type not allowed! Expected: {allowed_operators} got: {parameters.get('operator2')} ."
                    )
                if not parameters.get("value2"):
                    raise ValueError("Value2 for operator2 is missing!")
                if not parameters.get("operators_conjunction"):
                    raise ValueError(
                        f"Operator for conjunction is missing! Expected: {allowed_conjunction} got empty."
                    )
                if parameters.get("operators_conjunction") not in allowed_conjunction:
                    raise ValueError(
                        f"Operator for conjunction not allowed! Expected: {allowed_conjunction} got {parameters.get('operators_conjunction')} ."
                    )
            if parameters.get("filters_conjunction"):
                if (
                    len(filters.keys()) == 1
                    and parameters.get("filters_conjunction") is not None
                ):
                    raise ValueError(
                        f"Filters conjunction allowed only when more then one filter provided!"
                    )
                if parameters.get("filters_conjunction") not in allowed_conjunction:
                    raise ValueError(
                        f"Filter operator for conjunction not allowed! Expected: {allowed_conjunction} got {parameters.get('filters_conjunction')} ."
                    )

        return True

    def operators_mapping(
        self,
        filters: dict,
    ) -> dict:
        """
        Function for mapping comparison and conjunction(logical) operators of filters to the format which is recognized by Microsoft API.
        Allowed operators:
            <
            >
            <=
            >=
            ==
            !=
            "&"
            "|"

        Args:
            filters (dict): A dictionary which contains operators.

        Raises:
            ValueError: If operator1 not allowed.
            ValueError: If operator2 not allowed.
            ValueError: If operators conjunction not allowed.
            ValueError: If filters conjunction not allowed.

        Returns:
            dict: New modified dict with mapped operators.
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
            if parameters.get("operators_conjunction"):
                logical_op_to_change = parameters.get("operators_conjunction")
                if logical_op_to_change in logical_op.keys():
                    parameters["operators_conjunction"] = logical_op[
                        logical_op_to_change
                    ]
                else:
                    raise ValueError(
                        f"This conjunction (logical) operator: {logical_op_to_change} is not allowed. Please read the function documentation for details!"
                    )
            if parameters.get("filters_conjunction"):
                logical_fl_to_change = parameters.get("filters_conjunction")
                if logical_fl_to_change in logical_op.keys():
                    parameters["filters_conjunction"] = logical_op[logical_fl_to_change]
                else:
                    raise ValueError(
                        f"This filters conjunction (logical) operator: {logical_fl_to_change} is not allowed. Please read the function documentation for details!"
                    )

        return filters_dict

    def make_filter_for_api(self, filters: dict) -> str:
        """
        Function changing type of operators to match MS API style as 'str' passing to URL call.

        Args:
            filters (dict): A dictionary which contains operators.

        Returns:
            str: Output as filtering string to pass as filter parameter to API.
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
                        + f" {parameters.get('operators_conjunction')} {column} {parameters.get('operator2')} datetime'{from_date2}' "
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
            if parameters.get("filters_conjunction"):
                filter_text = filter_text + f"{parameters.get('filters_conjunction')} "

        return filter_text

    def make_filter_for_df(
        self,
        filters: dict,
    ) -> str:
        """
        Function changing filters into pandas DataFrame filtering string used later for filtering the DF.

        Args:
            filters (dict): A dictionary which contains operators.

        Returns:
            str: Output as string to pass as filter to DataFrame.
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
                    + f") {parameters.get('operators_conjunction')} (df.{column} {parameters.get('operator2', '')} '{parameters.get('value2', '')}'"
                )

            if parameters.get("filters_conjunction"):
                filter_in_df = (
                    filter_in_df + ")" + parameters.get("filters_conjunction")
                )

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
        If filters are passed, function will try to extract only filtered data to reduce the amount of data to transfer.
        If there is no filter or there is an throttling (max rows returned limit reached)
            exception ,then 2nd workflow will start and download all data which will be filtered later in the data frame.

        Args:
            list_title (str): Title of Sharepoint List. Default to None.
            site_url (str): URL to set of Sharepoint Lists. Default to None.
            required_fields (List[str]): Required fields(columns) need to be extracted from
                                         Sharepoint List. Default to None.
            field_property (List[str]): Property to expand with expand query method.
                                        All properties can be found under list.item.properties.
                                        Default to ["Title"]
            filters (dict): Dictionary with operators which filters the SharepointList output.
                            allowed dtypes: ('datetime','date','bool','int', 'float', 'complex', 'str')
                            allowed conjunction: ('&','|')
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
                                    'operators_conjunction':'&',
                                    'filters_conjunction':'&',
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

        Raises:
            AttributeError: If filter column not included inside required fields list.
            ValueError: If there is no filter passed - > will extract all fields and filter later.

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
                        f"Filter '{key}' column not included inside required fields. It is obligatory to extract data which is filtered!"
                    )

            # changing the body of the filter for MS API call
            filter_text = self.make_filter_for_api(filters)

        download_all = False

        # extracting required_fields SP_List objects
        selected_fields = self.select_fields(
            list_title=list_title,
            site_url=site_url,
            required_fields=required_fields,
            field_property=field_property,
        )

        try:
            # Extract data below 5k rows or max limitation of the specific SP List with basic filtering.
            if filters is None:
                raise ValueError("There is no filter. Switching to extract all fields.")
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
            # Extract all data from specific SP List without basic filtering. Additional logic for filtering applied on DataFrame level.
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
            # Apply filters to the data frame -> accordingly to the filter dict passed as na parameter
            self.logger.info("Filtering df with all data output")
            filter_for_df = self.make_filter_for_df(filters)
            df = eval(filter_for_df)

        return df
