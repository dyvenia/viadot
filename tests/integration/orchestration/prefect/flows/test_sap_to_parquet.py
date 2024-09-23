from pathlib import Path

import pandas as pd

from viadot.orchestration.prefect.flows import sap_to_parquet


PATH = "test_path.parquet"
SAP_CREDS = "sap-dev"


def test_sap_to_parquet():
    assert not Path(PATH).exists()

    sap_to_parquet(
        path=PATH,
        query="""SELECT MATKL, MTART, ERSDA FROM MARA LIMIT 100""",
        func="RFC_READ_TABLE",
        rfc_total_col_width_character_limit=400,
        sap_credentials_secret=SAP_CREDS,
    )

    assert Path(PATH).exists()

    df = pd.read_parquet(PATH)
    n_row = 100
    n_col = 5

    assert (n_row, n_col) == df.shape
    Path(PATH).unlink()
