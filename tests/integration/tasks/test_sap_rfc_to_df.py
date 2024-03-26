import pytest
import logging

from viadot.exceptions import CredentialError
from viadot.config import local_config
from viadot.tasks import SAPRFCToDF


def test_sap_rfc_to_df_bbp():
    task = SAPRFCToDF(
        query="SELECT MATNR, MATKL, MTART, LAEDA FROM MARA WHERE LAEDA LIKE '20220110%'",
        func="BBP_RFC_READ_TABLE",
    )
    df = task.run(sap_credentials_key="SAP", env="QA")
    assert len(df.columns) == 4 and not df.empty


def test_sap_rfc_to_df_wrong_sap_credential_key_bbp(caplog):
    task = SAPRFCToDF(
        query="SELECT MATNR, MATKL, MTART, LAEDA FROM MARA WHERE LAEDA LIKE '20220110%'",
        func="BBP_RFC_READ_TABLE",
    )
    with pytest.raises(
        CredentialError,
        match="Sap_credentials_key: SAP_test is not stored neither in KeyVault or Local Config!",
    ):
        task.run(
            sap_credentials_key="SAP_test",
        )
        assert (
            f"Getting credentials from Azure Key Vault was not possible. Either there is no key: SAP_test or env: DEV or there is not Key Vault in your environment."
            in caplog.text
        )


def test_sap_rfc_to_df_wrong_env_bbp(caplog):
    task = SAPRFCToDF(
        query="SELECT MATNR, MATKL, MTART, LAEDA FROM MARA WHERE LAEDA LIKE '20220110%'",
        func="BBP_RFC_READ_TABLE",
    )
    with pytest.raises(
        CredentialError,
        match="Missing PROD_test credentials!",
    ):
        task.run(
            sap_credentials_key="SAP",
            env="PROD_test",
        )
        assert (
            f"Getting credentials from Azure Key Vault was not possible. Either there is no key: SAP or env: PROD_test or there is not Key Vault in your environment."
            in caplog.text
        )
