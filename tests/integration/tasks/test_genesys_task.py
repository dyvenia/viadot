from viadot.tasks import GenesysToCSV, GenesysToDF

import os
from unittest import mock

import pandas as pd
import pytest

MEDIA_TYPE_LIST = ["callback"]

QUEUEIDS_LIST = ["780807e6-83b9-44be-aff0-a41c37fab004"]

DATA_TO_POST_STR = """{
    "name": f"QUEUE_PERFORMANCE_DETAIL_VIEW_{media}",
    "timeZone": "UTC",
    "exportFormat": "CSV",
    "interval": f"{end_date}T23:00:00/{start_date}T23:00:00",
    "period": "PT30M",
    "viewType": f"QUEUE_PERFORMANCE_DETAIL_VIEW",
    "filter": {"mediaTypes": [f"{media}"], "queueIds": [f"{queueid}"], "directions":["inbound"],},
    "read": True,
    "locale": "en-us",
    "hasFormatDurations": False,
    "hasSplitFilters": True,
    "excludeEmptyRows": True,
    "hasSplitByMedia": True,
    "hasSummaryRow": True,
    "csvDelimiter": "COMMA",
    "hasCustomParticipantAttributes": True,
    "recipientEmails": [],
    }"""

d = {"country": [1, 2], "sales": [3, 4]}
df = pd.DataFrame(data=d)

genesys_to_csv = GenesysToCSV()
genests_to_df = GenesysToDF()


def test_genesys_to_csv():
    with mock.patch.object(
        genesys_to_csv,
        "run",
        return_value=["V_D_PROD_FB_QUEUE_CALLBACK.csv", "V_D_PROD_FB_QUEUE_CHAT.csv"],
    ) as mock_method:

        files_name_list = genesys_to_csv.run(
            media_type_list=MEDIA_TYPE_LIST,
            queueIds_list=QUEUEIDS_LIST,
            data_to_post_str=DATA_TO_POST_STR,
        )
        assert isinstance(files_name_list, list)


def test_genests_to_df():
    with mock.patch.object(
        genests_to_df,
        "run",
        return_value=df,
    ) as mock_method:

        final_df = genests_to_df.run(
            report_url="https://apps.mypurecloud.de/platform/api/v2/downloads/62631b245c69f0c8",
        )
        assert isinstance(final_df, pd.DataFrame)
