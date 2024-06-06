import os

from viadot.orchestration.prefect.flows import sharepoint_to_redshift_spectrum
from viadot.sources import RedshiftSpectrum


def test_sharepoint_to_redshift_spectrum(sharepoint_url, sharepoint_credentials_secret):
    s3_bucket = os.environ.get("S3_BUCKET")
    test_schema = "raw_test"
    test_table = "test_sharepoint_to_redshift_spectrum"

    sharepoint_to_redshift_spectrum(
        sharepoint_url=sharepoint_url,
        to_path=f"s3://{s3_bucket}/nesso/{test_schema}/{test_table}",
        schema_name=test_schema,
        table=test_schema,
        sharepoint_credentials_secret=sharepoint_credentials_secret,
        download_all_files=False,
        return_as_one_table=False,
        file_sheet_mapping={},
    )

    rs = RedshiftSpectrum()

    received_df = rs.to_df(database=test_schema, table=test_table)

    assert not received_df.empty

    rs.rm(database=test_schema, table=test_table)
