from viadot.orchestration.prefect.tasks import exchange_rates_to_df


expected_df = [
    [
        "2022-10-07",
        "PLN",
        0.200449,
        0.205797,
        0.180677,
        0.199348,
        1,
        1.530772,
        926.075683,
        5.038713,
        2.247641,
        2.147416,
        28.902797,
    ],
    [
        "2022-10-08",
        "PLN",
        0.200369,
        0.205714,
        0.180732,
        0.199321,
        1,
        1.530195,
        929.700228,
        5.036524,
        2.249218,
        2.14749,
        28.891214,
    ],
    [
        "2022-10-09",
        "PLN",
        0.200306,
        0.205766,
        0.180915,
        0.199232,
        1,
        1.530656,
        925.417886,
        5.03927,
        2.249399,
        2.146299,
        28.910243,
    ],
]


def test_exchange_rates_to_df_task():
    df = exchange_rates_to_df.fn(
        currency="PLN",
        config_key="exchange_rates_dev",
        start_date="2022-10-07",
        end_date="2022-10-09",
    )
    df.drop(["_viadot_downloaded_at_utc"], axis=1, inplace=True)
    assert df == expected_df
