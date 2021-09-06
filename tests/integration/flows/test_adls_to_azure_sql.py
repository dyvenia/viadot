from viadot.flows import ADLSToAzureSQL


def test_get_promoted_adls_path_file():
    adls_path_file = "raw/supermetrics/adls_ga_load_times_fr_test/2021-07-14T13%3A09%3A02.997357%2B00%3A00.csv"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_file)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_file_starts_with_slash():
    adls_path_dir_starts_with_slash = "/raw/supermetrics/adls_ga_load_times_fr_test/"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir_starts_with_slash)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_dir_slash():
    adls_path_dir_slash = "raw/supermetrics/adls_ga_load_times_fr_test/"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir_slash)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_dir():
    adls_path_dir = "raw/supermetrics/adls_ga_load_times_fr_test"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"


def test_get_promoted_adls_path_dir_starts_with_slash():
    adls_path_dir_starts_with_slash = "/raw/supermetrics/adls_ga_load_times_fr_test/"
    flow = ADLSToAzureSQL(name="test", adls_path=adls_path_dir_starts_with_slash)
    promoted_path = flow.get_promoted_path(env="conformed")
    assert promoted_path == "conformed/supermetrics/adls_ga_load_times_fr_test.csv"
