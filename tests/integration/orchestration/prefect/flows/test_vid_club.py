from src.viadot.orchestration.prefect.flows import vid_club_to_adls
from src.viadot.sources import AzureDataLake


TEST_FILE_PATH = "test/path/to/adls.parquet"
TEST_SOURCE = "jobs"
TEST_FROM_DATE = "2023-01-01"
TEST_TO_DATE = "2023-12-31"
ADLS_CREDENTIALS_SECRET = "test_adls_secret"  # pragma: allowlist secret # noqa: S105
VIDCLUB_CREDENTIALS_SECRET = (
    "test_vidclub_secret"  # pragma: allowlist secret # noqa: S105
)


def test_vid_club_to_adls():
    lake = AzureDataLake(config_key="adls_test")

    assert not lake.exists(TEST_FILE_PATH)

    vid_club_to_adls(
        endpoint=TEST_SOURCE,
        from_date=TEST_FROM_DATE,
        to_date=TEST_TO_DATE,
        adls_path=TEST_FILE_PATH,
        adls_azure_key_vault_secret=ADLS_CREDENTIALS_SECRET,
        vidclub_credentials_secret=VIDCLUB_CREDENTIALS_SECRET,
    )

    assert lake.exists(TEST_FILE_PATH)

    lake.rm(TEST_FILE_PATH)
