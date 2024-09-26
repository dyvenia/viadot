from prefect import flow

from viadot.orchestration.prefect.tasks import sap_rfc_to_df


def test_sap_rfc_to_df():
    @flow
    def test_flow():
        return sap_rfc_to_df(
            config_key="sap_test",
            query="SELECT MATKL, MTART, ERSDA FROM MARA WHERE ERSDA = '20221230'",
            func="BBP_RFC_READ_TABLE",
        )

    received_df = test_flow()
    n_cols = 3

    assert not received_df.empty
    assert len(received_df.columns) == n_cols
