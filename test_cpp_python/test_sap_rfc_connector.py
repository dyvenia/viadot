import sap_rfc_connector
import pandas as pd
import numpy as np
import time

def _gen_split(data, sep, record_key):
    for row in data:
        yield row[record_key].split(sep)

if __name__ == "__main__":
    # Replace these with actual SAP credentials
    user = "ZWEBSERVICE"
    passwd = "4YVj745z"
    ashost = "10.120.222.3"
    sysnr = "01"  # Example system number
    sep = "☩"
    
    connector = sap_rfc_connector.SapRfcConnector()
    connected = connector.connect(user, passwd, ashost, sysnr)
    print(f"Connected: {connected}")

    print(connector.get_library_info())
    # start timer
    start_time = time.time()
    if connected:
        alive = connector.check_connection()
        print(f"Connection alive: {alive}")

        # Create the function caller
        func = sap_rfc_connector.SapFunctionCaller(connector)

        #### DESCRIPTION
        # descr = func.get_function_description("BBP_RFC_READ_TABLE")
        # print(descr)
        #### END DESCRIPTION
        # print(connector.get_table_metadata("KONP"))
        response = func.rfc_read_table_query("SELECT KNUMH, KOPOS, KAPPL, KSCHL, KBETR FROM KONP WHERE KAPPL='KA'", sep)

        #### TABLE METADATA
        # response = func.get_table_metadata("ZSV_ZEQUZ")
        # print(np.array(response))
        # exit()
        #### END TABLE METADATA

        #### READ TABLE
        # response = func.rfc_read_table_query(
        #     "SELECT MATNR, SERNR, ERDAT, AEDAT, AENAM FROM ZSV_ZEQUZ WHERE MATNR = '484CE1I32100' AND SERNR = '11110189C'",
        #     sep="☩",
        #     rowcount=10
        # )
        # print(response)
        if response["DATA"]:
            record_key = "WA"
            data_raw = np.array(response["DATA"])
            # print(data_raw)
            records = list(_gen_split(data_raw, sep, record_key))
            df = pd.DataFrame(records)
            print(df)
        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")
        connector.close_connection()
        print("Connection closed.")
    else:
        print("Failed to connect to SAP.") 