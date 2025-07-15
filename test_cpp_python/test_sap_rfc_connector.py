import sap_rfc_connector
import pandas as pd
import numpy as np
from typing import Iterable, Iterator
import time

def _gen_split(data: Iterable[str], sep: str, record_key: str) -> Iterator[list[str]]:
    for row in data:
        yield row[record_key].split(sep)

if __name__ == "__main__":
    # Replace these with actual SAP credentials
    user = "ZWEBSERVICE"
    passwd = "4YVj745z"
    ashost = "10.120.222.3"
    sysnr = "01"  # Example system number

    connector = sap_rfc_connector.SapRfcConnector()
    connected = connector.connect(user, passwd, ashost, sysnr)
    print(f"Connected: {connected}")

    print(connector.get_library_info())
    #start timer
    start_time = time.time()    
    if connected:
        alive = connector.check_connection()
        print(f"Connection alive: {alive}")
        #### DESCRIPTION
        descr = connector.get_function_description("BBP_RFC_READ_TABLE")
        # print(descr)
        param_names = [param["name"] for param in descr.parameters]
        # print(param_names)
        detailed_params = descr.parameters
        filtered_detailed_params = [
            {
                "name": param["name"],
                "parameter_type": param["parameter_type"],
                "default_value": param["default_value"],
                "optional": param["optional"],
                "parameter_text": param["parameter_text"],
            }
            for param in descr.parameters
        ]

        params = filtered_detailed_params
        params = pd.DataFrame.from_records(params)
        print(params)
        #### END DESCRIPTION
        # print(connector.get_table_metadata("KONP"))
        # response = connector.rfc_read_table_query("SELECT KNUMH, KOPOS, KAPPL, KSCHL, KBETR FROM KONP WHERE KAPPL='KA'", "♔")

        #ORIGINAL sap_rfc CODE
    #     if response["DATA"]:
    #         record_key = "WA"
    #         data_raw = np.array(response["DATA"])
    #         del response
    #         sep = "♔"

    #         records = list(_gen_split(data_raw, sep, record_key))
    #         del data_raw
    #         df = pd.DataFrame(records)
    #         print(df)
    #     end_time = time.time()
    #     print(f"Time taken: {end_time - start_time} seconds")
    #     connector.close_connection()
    #     print("Connection closed.")
    # else:
    #     print("Failed to connect to SAP.") 