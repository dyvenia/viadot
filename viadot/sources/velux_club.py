import pandas as pd
import requests

pd.set_option("display.max_columns", None)

API_URL = "https://api.club.velux.com/api/v1/datalake/"
TOKEN = "k7h8MvfoSxdpCMdZNval8SoGtyzoiM9SjSE03QgnYxOBEztw4YIQkJxRLKjXBnuB"


def velux_club_to_df(
    source: list, from_date: str = "", to_date: str = "", region="null"
) -> tuple:
    """Function to download a file from sharepoint, given its URL
    Args:
        source (str):
        from_date (str):
        to_date (str,str):
        region (str):
    Returns:
        str: filename

    """
    df = pd.DataFrame()

    if source in ["jobs", "product", "company"]:
        # check if date filter was passed!
        if from_date == "" or to_date == "":
            return (df, "Introduce a 'FROM Date' and 'To Date'")
        url = f"{API_URL}{source}?from={from_date}&to={to_date}&region&limit=100"
    elif source in "survey":
        url = f"{API_URL}{source}?language=en&type=question"
    else:
        return (df, "pick one these sources: jobs, product, company, survey")

    headers = {
        "Authorization": "Bearer " + TOKEN,
        "Content-Type": "application/json",
    }

    r = requests.request("GET", url, headers=headers)

    response = r.json()

    if isinstance(response, dict):
        keys_list = list(response.keys())
    elif isinstance(response, list):
        keys_list = list(response[0].keys())
    else:
        keys_list = []

    if "data" in keys_list:
        # first page content
        df = pd.DataFrame(response["data"])
        # next pages
        while response["next_page_url"] != None:
            url = f"{response['next_page_url']}&from={from_date}&to={to_date}&region&limit=100"
            r = requests.request("GET", url, headers=headers)
            response = r.json()
            df_page = pd.DataFrame(response["data"])
            df_page_transpose = df_page.T
            df = df.append(df_page_transpose, ignore_index=True)

    else:
        df = pd.DataFrame(response)

    # file_name = f"{source}_from_{from_date}_to_{to_date}.parquet"
    # df.astype(str).to_parquet(file_name)
    return (df, f"Source {source}")


def print_df(df: pd.DataFrame, service_name: str):
    print(f"{service_name} Dataframe Columns")
    print(list(df.columns))
    print(f"{service_name} Number of Columns")
    print(len(list(df.columns)))
    print(f"{service_name} Dataframe Number Of Samples")
    print(str(len(df.index)))


(df, msg) = velux_club_to_df("jobs", "2023-01-01", "2023-01-03")
print_df(df, "Jobs")

(df, msg) = velux_club_to_df("product", "2023-01-01", "2023-01-03")
print_df(df, "Product")

(df, msg) = velux_club_to_df("company", "2023-01-01", "2023-01-03")
print_df(df, "Company")

(df, msg) = velux_club_to_df("survey")
print_df(df, "Survey")
