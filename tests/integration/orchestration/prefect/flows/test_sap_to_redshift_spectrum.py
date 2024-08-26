import os

from viadot.orchestration.prefect.flows import sap_to_redshift_spectrum
from viadot.sources import RedshiftSpectrum


S3_BUCKET = os.environ.get("S3_BUCKET")
TEST_SCHEMA = "raw_test"
TEST_TABLE = "test_sap_to_redshift_spectrum"


def test_sap_to_redshift_spectrum():
    sap_to_redshift_spectrum(
        to_path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}",
        schema_name=TEST_SCHEMA,
        table=TEST_TABLE,
        query="SELECT MATKL, MTART, ERSDA FROM MARA WHERE ERSDA = '20221230'",
        func="BBP_RFC_READ_TABLE",
        sap_config_key="sap_test",
    )

    rs = RedshiftSpectrum()

    received_df = rs.to_df(database=TEST_SCHEMA, table=TEST_TABLE)

    n_cols = 3
    assert not received_df.empty
    assert len(received_df.columns) == n_cols

    rs.rm(database=TEST_SCHEMA, table=TEST_TABLE)
