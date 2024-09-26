import os

import pandas as pd
from prefect import flow
import pytest

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum
from viadot.utils import skip_test_on_missing_extra


try:
    from viadot.sources import RedshiftSpectrum
except ImportError:
    skip_test_on_missing_extra(source_name="RedshiftSpectrum", extra="aws")


S3_BUCKET = os.environ.get("S3_BUCKET")
TEST_SCHEMA = "raw_test"
TEST_TABLE = "test_sap_to_redshift_spectrum"


@pytest.fixture(scope="session")
def redshift(aws_config_key):
    return RedshiftSpectrum(config_key=aws_config_key)


def test_df_to_redshift_spectrum():
    df = pd.DataFrame(
        [
            [0, "A"],
            [1, "B"],
            [2, "C"],
        ],
        columns=["col1", "col2"],
    )

    @flow
    def test_flow(df):
        df_to_redshift_spectrum(
            df=df,
            to_path=f"s3://{S3_BUCKET}/nesso/{TEST_SCHEMA}/{TEST_TABLE}",
            schema_name=TEST_SCHEMA,
            table=TEST_TABLE,
        )

        df_exists = redshift._check_if_table_exists(
            schema=TEST_SCHEMA,
            table=TEST_TABLE,
        )

        redshift.drop_table(schema=TEST_SCHEMA, table=TEST_TABLE)

        return df_exists

    assert test_flow(df)
