import json
from datetime import datetime
from typing import Any, Dict, Literal, List

import pandas as pd
from prefect import Task
from prefect.utilities import logging
from prefect.utilities.tasks import defaults_from_attrs

from viadot.sources.customer_gauge import CustomerGauge
from viadot.tasks.azure_key_vault import AzureKeyVaultSecret

logger = logging.get_logger()


class CustomerGaugeToDF(Task):
    def __init__(
        self,
        endpoint: Literal["responses", "non-responses"] = None,
        total_load: bool = True,
        endpoint_url: str = None,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        method1_cols: List[str] = None,
        method2_cols: List[str] = None,
        timeout: int = 3600,
        *args,
        **kwargs,
    ):
        """
        Task CustomerGaugeToDF for downloading the selected range of data from Customer Gauge 
        endpoint and return as one pandas DataFrame.

        Args:
            endpoint (Literal["responses", "non-responses"], optional): Indicate which endpoint 
                to connect. Defaults to None.
            total_load (bool, optional): Indicate whether to download the data to the latest. 
                If 'False', only one API call is executed (up to 1000 records). Defaults to True.
            endpoint_url (str, optional): Endpoint URL. Defaults to None.
            cursor (int, optional): Cursor value to navigate to the page. Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page, max value = 1000. 
                Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent", "date_survey_response"], 
                optional): Specifies the date type which filter date range. Defaults to None.
            start_date (datetime, optional): Defines the period end date in yyyy-mm-dd format. 
                Defaults to None.
            end_date (datetime, optional): Defines the period start date in yyyy-mm-dd format. 
                Defaults to None.
            method1_cols (List[str]): Columns to unpack and modify using `_field_reference_unpacker`.
                Defaults to None.
            method2_cols (List[str]): Columns to unpack and modify using `_nested_dict_transformer`.
                Defaults to None.
            timeout (int, optional): The time (in seconds) to wait while running this task before 
                a timeout occurs. Defaults to 3600.
        """
        self.endpoint = endpoint
        self.total_load = total_load
        self.endpoint_url = endpoint_url
        self.cursor = cursor
        self.pagesize = pagesize
        self.date_field = date_field
        self.start_date = start_date
        self.end_date = end_date
        self.method1_cols = method1_cols
        self.method2_cols = method2_cols

        super().__init__(
            name="customer_gauge_to_df",
            timeout=timeout,
            *args,
            **kwargs,
        )
    def get_data(self, 
        json_response: Dict[str, Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract and return the 'data' part of a JSON response as a list of dictionaries.

        Args:
            json_response (Dict[str, Any], optional): JSON object represented as a nested 
            dictionary that contains data and cursor parameter value. Defaults to None.

        Raises:
            KeyError: If the 'data' key is not present in the provided JSON response.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing data from the 'data' 
            part of the JSON response.
        """
        jsons_list=[]
        try:
            jsons_list = json_response["data"]
        except KeyError:
            logger.error(
                "Provided argument doesn't contain 'data' value. Pass json returned from the endpoint."
            )
            raise

        return jsons_list

    def _field_reference_unpacker(
        self, 
        json_response: Dict[str, Any],
        field: str,
    ) -> Dict[str, Any]:
        """
        Unpack and modify dictionaries within the specified field of a JSON response.

        This function takes a JSON response and a field name. It processes dictionaries
        within the specified field, checking if each dictionary contains exactly two items.
        If a dictionary meets this criteria, it is transformed into a new dictionary, 
        where the first key becomes a key, and the second key becomes its associated value

        Args:
            json_response (Dict[str, Any], optional): JSON response with data.
            field (str): The key (column) of the dictionary to be modified.

        Returns:
            Dict[str, Any]: The JSON response with modified nested dictionaries
            within the specified field.
            
        Raises:
            ValueError: If a dictionary within the specified field doesn't contain exactly two items.
        """

        result = {}
        for i, dictionary in enumerate(json_response[field]):
            if isinstance(dictionary, dict) and len(dictionary.items()) == 2:
                list_properties = list(dictionary.values())
                result[list_properties[0]] = list_properties[1]
            else:
                raise ValueError(f"Dictionary within the specified field doesn't contain exactly two items.")
        if result:
            json_response[field] = result

        return json_response

    def _nested_dict_transformer(
        self, 
        json_response: Dict[str, Any],
        field: str,
    ) -> Dict[str, Any]:
        """
        Modify nested dictionaries within the specified field of a JSON response.

        This function takes a JSON response and a field name. It modifies nested
        dictionaries within the specified field by adding an index and underscore
        to the keys. The modified dictionary is then updated in the JSON response.

        Args:
            json_response (Dict[str, Any], optional): JSON response with data.
            field (str): The key (column) of the dictionary to be modified.

        Returns:
            Dict[str, Any]: The JSON response with modified nested dictionaries
        within the specified field.
        """
        result={}
        try:
            for i, dictionary in enumerate(json_response[field], start=1):
                for key, value in dictionary.items():
                    result[f'{i}_{key}'] = value
            if result:
                json_response[field] = result
        except TypeError as te:
            logger.error(te)

        return json_response
    
    def column_unpacker(
        self, 
        json_list: List[Dict[str, Any]] = None,
        method1_cols: List[str] = None,
        method2_cols: List[str] = None,
        ) -> List[Dict[str, Any]]:

        """
        Function to unpack and modify specific columns in a list of dictionaries by using one of two methods, 
        chosen by the user. 
        If user would like to use field_reference_unpacker, he/she needs to provide list of fields as strings in 
        `method1_cols`  parameter,  if user would like to use nested_dict_transformer he/she needs to provide list of 
         fields as strings in method2_cols parameter.  

        Args:
            json_list (List[Dict[str, Any]): A list of dictionaries containing the data.
            method1_cols (List[str]): Columns to unpack and modify using `_field_reference_unpacker`. 
                Defaults to None.
            method2_cols (List[str]): Columns to unpack and modify using `_nested_dict_transformer`. 
                Defaults to None.

        Raises:
            ValueError: If 'json_list' is not provided.
            ValueError: If specified columns do not exist in the JSON data.
            ValueError: If columns are mentioned in both 'method1_cols' and 'method2_cols'. 

        Returns:
            List[Dict[str, Any]]: The updated list of dictionaries after column unpacking and modification.
        """
        duplicated_cols = []
        
        if json_list is None:
            raise ValueError("Input 'json_list' is required.")

        def unpack_columns(columns, unpack_function):
            json_list_clean = json_list.copy()
            for field in columns:
                if field in json_list_clean[0]:
                    logger.info(f"Unpacking column '{field}' with {unpack_function.__name__} method...")
                    try:
                        json_list_clean = list(map(lambda x: unpack_function(x, field), json_list_clean))
                        logger.info(f"All elements in '{field}' are unpacked successfully.")
                    except ValueError as ve:
                        logger.info(f"No transformation were made in '{field}'," 
                        "because didn't contain list of key-value data.")
                    except Exception as e:
                        logger.info(f"Error while unpacking {field}: {e}")
                else:
                    logger.info(f"Column '{field}' not found.")
            return json_list_clean
        if method1_cols and method2_cols:
            duplicated_cols = set(method1_cols).intersection(set(method2_cols))
        if duplicated_cols:
            raise ValueError(
                f"{duplicated_cols} were mentioned in both method1_cols and method2_cols." 
                " It's not possible to apply two methods to the same field."
                )
        else:
            if method1_cols is not None:
                json_list = unpack_columns(
                    columns = method1_cols, 
                    unpack_function = self._field_reference_unpacker
                    )

            if method2_cols is not None:
                json_list = unpack_columns(
                    columns = method2_cols, 
                    unpack_function = self._nested_dict_transformer
                    )
        
        return json_list


    def flatten_json(self, json_response: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Function that flattens a nested structure of the JSON object into 
        a single-level dictionary. It uses a nested `flattify()` function to recursively 
        combine nested keys in the JSON object with '_' to create the flattened keys.

        Args:
            json_response (Dict[str, Any], optional): JSON object represented as 
            a nested dictionary. Defaults to None.

        Raises:
            TypeError: If the 'json_response' not a dictionary.

        Returns:
            Dict[str, Any]: The flattened dictionary.
        """
        result = {}

        if not isinstance(json_response, dict):
            raise TypeError("Input must be a dictionary.")

        def flattify(field, key="", out = None):
            if out is None:
                out = result

            if isinstance(field, dict):
                for item in field.keys():
                    flattify(field[item], key + item + "_", out)
            else:
                out[key[:-1]] = field

        flattify(json_response)

        return result
      
    def square_brackets_remover(
        self, 
        df: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Replace square brackets "[]" with an empty string in a pandas DataFrame.

        Args:
            df (pd.DataFrame, optional): Replace square brackets "[]" with an empty string 
            in a pandas DataFrame. Defaults to None.

        Returns:
            pd.DataFrame: The modified DataFrame with square brackets replaced by an empty string.
        """

        df = df.astype(str)
        df = df.applymap(lambda x: x.strip("[]"))
        return df
    
    def _drivers_cleaner(
        self,
        drivers: str = None
    ) -> str:
        """
        Clean and format the 'drivers' data.

        Args:
            drivers (str, optional): Column name of the data to be cleaned. Defaults to None.

        Returns:
            str: A cleaned and formatted string of driver data.
        """

        drivers = drivers.split("}, {")
        cleaned_drivers = []
        for driver in drivers:
            driver = driver.replace("{", "").replace("}", "")
            driver = driver.replace("'", "").replace("label: ", "")
            cleaned_drivers.append(driver)
        return ', '.join(cleaned_drivers)  

    def __call__(self):
        """Download Customer Gauge data to a DF"""
        super().__call__(self)

    @defaults_from_attrs(
        "endpoint",
        "total_load",
        "endpoint_url",
        "cursor",
        "pagesize",
        "date_field",
        "start_date",
        "end_date",
        "method1_cols",
        "method2_cols",
    )
    def run(
        self,
        endpoint: Literal["responses", "non-responses"] = None,
        total_load: bool = True,
        endpoint_url: str = None,
        cursor: int = None,
        pagesize: int = 1000,
        date_field: Literal[
            "date_creation", "date_order", "date_sent", "date_survey_response"
        ] = None,
        start_date: datetime = None,
        end_date: datetime = None,
        method1_cols: List[str] = None,
        method2_cols: List[str] = None,
        credentials_secret: str = "CUSTOMER-GAUGE",
        vault_name: str = None,
    ) -> pd.DataFrame:
        """
        Run method. Downloading the selected range of data from Customer Gauge endpoint and return 
        as one pandas DataFrame.

        Args:
            endpoint (Literal["responses", "non-responses"]): Indicate which endpoint to connect. 
                Defaults to None.
            total_load (bool, optional): Indicate whether to download the data to the latest. If 
                'False', only one API call is executed (up to 1000 records). Defaults to True.
            endpoint_url (str, optional): Endpoint URL. Defaults to None.
            cursor (int, optional): Cursor value to navigate to the page. Defaults to None.
            pagesize (int, optional): Number of responses (records) returned per page, max value = 1000. 
                Defaults to 1000.
            date_field (Literal["date_creation", "date_order", "date_sent", "date_survey_response"], 
                optional): Specifies the date type which filter date range. Defaults to None.
            start_date (datetime, optional): Defines the period end date in yyyy-mm-dd format. 
                Defaults to None.
            end_date (datetime, optional): Defines the period start date in yyyy-mm-dd format. 
                Defaults to None.
            method1_cols (List[str]): Columns to unpack and modify using `_field_reference_unpacker`. 
                Defaults to None.
            method2_cols (List[str]): Columns to unpack and modify using `_nested_dict_transformer`. 
                Defaults to None.       
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a 
                dictionary with ['client_id', 'client_secret']. Defaults to "CUSTOMER-GAUGE".
            vault_name (str, optional): The name of the vault from which to obtain the secret. 
                Defaults to None.

        Returns:
            pd.DataFrame: Final pandas DataFrame.
        """
        try:
            credentials_str = AzureKeyVaultSecret(
                credentials_secret, vault_name=vault_name
            ).run()
            credentials = json.loads(credentials_str)
        except (ValueError, TypeError) as e:
            logger.error(e)

        total_json = []

        customer_gauge = CustomerGauge(
            endpoint=endpoint, url=endpoint_url, credentials=credentials
        )
        logger.info(
            f"Starting downloading data from {self.endpoint or self.endpoint_url} endpoint..."
        )
        json_data = customer_gauge.get_json_response(
            cursor=cursor,
            pagesize=pagesize,
            date_field=date_field,
            start_date=start_date,
            end_date=end_date,
        )
        cur = customer_gauge.get_cursor(json_data)

        jsn = self.get_data(json_data)
        total_json += jsn

        if total_load == True:
            if cursor is None:
                logger.info(
                    f"Downloading all the data from the {self.endpoint or self.endpoint_url} endpoint." 
                    "Process might take a few minutes..."
                )
            else:
                logger.info(
                    f"Downloading starting from the {cursor} cursor. Process might take a few minutes..."
                )
            while jsn:
                json_data = customer_gauge.get_json_response(cursor=cur)
                cur = customer_gauge.get_cursor(json_data)
                jsn = self.get_data(json_data)
                total_json += jsn

        clean_json = self.column_unpacker(
            json_list = total_json, 
            method1_cols = method1_cols, 
            method2_cols = method2_cols)
        logger.info("Inserting data into the DataFrame...")
        df = pd.DataFrame(list(map(self.flatten_json, clean_json)))
        df = self.square_brackets_remover(df)
        if "drivers" in list(df.columns):
            df["drivers"] = df["drivers"].apply(self._drivers_cleaner)
        df.columns = df.columns.str.lower().str.replace(" ", "_")
        logger.info("DataFrame: Ready. Data: Inserted. Let the magic happen!")

        return df
