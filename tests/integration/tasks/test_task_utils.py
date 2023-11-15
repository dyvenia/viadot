import pandas as pd
from prefect.backend import get_key_value, set_key_value
from prefect.engine.state import Failed, Success
from prefect.tasks.secrets import PrefectSecret

from viadot.task_utils import custom_mail_state_handler, set_new_kv, check_value


def test_custom_state_handler():
    vault_name = PrefectSecret("AZURE_DEFAULT_KEYVAULT").run()

    final_state = custom_mail_state_handler(
        tracked_obj="Flow",
        old_state=Success,
        new_state=Failed,
        only_states=[Failed],
        local_api_key=None,
        credentials_secret="SENDGRIND",
        vault_name=vault_name,
    )

    assert final_state == Failed


def test_set_new_kv():
    df = pd.DataFrame(data={"col1": [1, 72, 24, 2], "col2": [0, 0, 3, 4]})
    set_new_kv.run(kv_name="test_for_setting_kv", df=df, filter_column="col1")
    result = get_key_value("test_for_setting_kv")
    assert result == "72"
    set_key_value(key="test_for_setting_kv", value=None)


# Sample test checking the correctness of the function when the key is found
def test_check_value_found():
    json_data = {
        "first_known_lvl": {
            "second_known_lvl": {
                "third_known_lvl": {
                    "searched_phrase": "phrase"
                }
            }
        }
    }
    result = check_value(json_data["first_known_lvl"]["second_known_lvl"]["third_known_lvl"], ["searched_phrase"])
    assert result == "phrase"

# Sample test checking the correctness of the function when the key is not found
def test_check_value_not_found():
    json_data = {
        "first_known_lvl": {
            "second_known_lvl": {
                "third_known_lvl": {
                    "other_phrase": "This won't be found"
                }
            }
        }
    }
    result = check_value(json_data["first_known_lvl"]["second_known_lvl"]["third_known_lvl"], ["searched_phrase"])
    assert result is None

# Sample test checking the correctness of the function with an empty dictionary
def test_check_value_empty_dict():
    json_data = {}
    result = check_value(json_data, ["searched_phrase"])
    assert result is None

# Sample test checking the correctness of the function with a nonexistent key
def test_check_value_nonexistent_key():
    json_data = {
        "first_known_lvl": {
            "second_known_lvl": {
                "third_known_lvl": {
                    "searched_phrase": "phrase"
                }
            }
        }
    }
    result = check_value(json_data, ["nonexistent_key"])
    assert result is None