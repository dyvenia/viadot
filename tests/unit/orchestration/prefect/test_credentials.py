from unittest.mock import MagicMock

from viadot.orchestration.prefect import utils as prefect_utils


def test_get_aws_credentials_supports_machine_level_auth(monkeypatch):
    """Return optional static keys as None for IRSA-backed AWS blocks."""
    aws_credentials_block = MagicMock(
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region_name="eu-west-1",
    )
    load_prefect_block = MagicMock(return_value=aws_credentials_block)
    monkeypatch.setattr(prefect_utils, "AwsCredentials", MagicMock(), raising=False)
    monkeypatch.setattr(
        prefect_utils,
        "_load_prefect_block",
        load_prefect_block,
    )

    credentials = prefect_utils._get_aws_credentials(
        "dp-aws-credentials",
        "AwsCredentials",
    )

    assert credentials == {
        "aws_access_key_id": None,
        "aws_secret_access_key": None,
        "region_name": "eu-west-1",
    }
