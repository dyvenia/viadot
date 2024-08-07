from viadot.orchestration.prefect.flows import sql_server_to_minio
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import MinIO


PATH = "data/tables_schemas.parquet"


def test_sql_server_to_minio():
    credentials = get_credentials("minio-dev")
    sql_server_to_minio(
        query=""" SELECT t.name as table_name
                ,s.name as schema_name
                FROM sys.tables t
                JOIN sys.schemas s
                ON t.schema_id = s.schema_id""",
        path="s3://datalake-dev/data/tables_schemas.parquet",
        sql_server_credentials_secret="sql-server",  # noqa: S106
        minio_credentials=credentials,
        basename_template="test-{i}",
        if_exists="overwrite_or_ignore",
    )

    minio = MinIO(credentials=credentials)
    file_exists = minio._check_if_file_exists(PATH)

    assert file_exists is True

    minio.rm(path=PATH)
