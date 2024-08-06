from viadot.orchestration.prefect.tasks import df_to_minio
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import MinIO

import pandas as pd


MINIO_CREDS = get_credentials("minio-dev")
PATH = "data/duckdb_test.parquet"

def test_df_to_minio():
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=d)
    task = df_to_minio(df=df, path=PATH, credentials = MINIO_CREDS)
    minio = MinIO(credentials=MINIO_CREDS)
    file_exists = minio._check_if_file_exists(PATH)

    assert file_exists == True

    minio.rm(path=PATH)
