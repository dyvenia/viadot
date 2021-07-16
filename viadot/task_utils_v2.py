from datetime import datetime, timezone
import pyarrow
import pandas as pd
from prefect import task

METADATA_COLUMNS = {"_viadot_downloaded_at_utc": "DATETIME"}


@task
def add_ingestion_metadata_task(
    path: str,
):
    """Add ingestion metadata columns, eg. data download date

    Args:
        path (str): The path to the CSV file containing the data.
        sep (str, optional): The separator to use when loading the file into the DataFrame. Defaults to "\t".
    """
    df = pd.read_parquet(path)
    df["_viadot_downloaded_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0)
    df.to_parquet(path, index=False)
