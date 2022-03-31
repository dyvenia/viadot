from viadot.tasks import BigQueryToDF
import pandas as pd


def test_bigquery_to_df():
    bigquery_to_df_task = BigQueryToDF()
    df = bigquery_to_df_task.run(
        project="manifest-geode-341308",
        dataset="official_empty",
        table="space",
        credentials_key="BIGQUERY_TESTS",
    )
    expectation_columns = ["date", "name", "count", "refresh"]

    assert isinstance(df, pd.DataFrame)
    assert expectation_columns == list(df.columns)
