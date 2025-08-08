import os

import moto
import pytest

from viadot.utils import skip_test_on_missing_extra


try:
    import boto3

    from viadot.sources import RedshiftSpectrum
except ImportError:
    skip_test_on_missing_extra("RedshiftSpectrum", extra="aws")


@pytest.fixture
def _aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
    os.environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105
    os.environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def _mocked_aws(_aws_credentials):
    """Mock all AWS interactions.

    Requires you to create your own boto3 clients.
    """
    with moto.mock_aws():
        yield


@pytest.fixture
def redshift_spectrum(_mocked_aws):
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="test_bucket")
    spectrum = RedshiftSpectrum(config_key="redshift_dev")
    spectrum.create_schema("test_schema")

    yield spectrum

    spectrum.drop_schema("test_schema")


@pytest.mark.skip(
    reason="To be implemented: https://github.com/dyvenia/viadot/issues/978."
)
@pytest.mark.usefixtures("_mocked_aws")
def test_from_df(redshift_spectrum, TEST_DF):
    bucket = "test_bucket"
    schema = "test_schema"
    table = "test_table"
    result = redshift_spectrum.from_df(
        df=TEST_DF,
        to_path=f"s3://{bucket}/{schema}/{table}",
        schema=schema,
        table=table,
    )
    assert result is True
