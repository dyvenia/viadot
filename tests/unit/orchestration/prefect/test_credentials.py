from unittest.mock import MagicMock

import pytest

from viadot.orchestration.prefect import utils as prefect_utils


def test_get_aws_credentials_supports_machine_level_auth(monkeypatch):
    """Omit static keys for IRSA-backed AWS blocks."""
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

    assert credentials == {"region_name": "eu-west-1"}


def test_get_aws_credentials_returns_complete_static_key_pair(monkeypatch):
    """Return both static keys when both are configured."""
    aws_secret_access_key = MagicMock()
    aws_secret_access_key.get_secret_value.return_value = "secret"
    aws_credentials_block = MagicMock(
        aws_access_key_id="access-key",
        aws_secret_access_key=aws_secret_access_key,
        region_name="eu-west-1",
    )
    monkeypatch.setattr(prefect_utils, "AwsCredentials", MagicMock(), raising=False)
    monkeypatch.setattr(
        prefect_utils,
        "_load_prefect_block",
        MagicMock(return_value=aws_credentials_block),
    )

    credentials = prefect_utils._get_aws_credentials(
        "dp-aws-credentials",
        "AwsCredentials",
    )

    assert credentials == {
        "aws_access_key_id": "access-key",
        "aws_secret_access_key": "secret",
        "region_name": "eu-west-1",
    }


def test_get_aws_credentials_rejects_partial_static_key_pair(monkeypatch):
    """Reject an access key without its matching secret key."""
    aws_credentials_block = MagicMock(
        aws_access_key_id="access-key",
        aws_secret_access_key=None,
        region_name="eu-west-1",
    )
    monkeypatch.setattr(prefect_utils, "AwsCredentials", MagicMock(), raising=False)
    monkeypatch.setattr(
        prefect_utils,
        "_load_prefect_block",
        MagicMock(return_value=aws_credentials_block),
    )

    with pytest.raises(ValueError, match="must be provided together"):
        prefect_utils._get_aws_credentials(
            "dp-aws-credentials",
            "AwsCredentials",
        )
