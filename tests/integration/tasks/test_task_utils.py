from viadot.task_utils import custom_mail_state_handler
from prefect.tasks.secrets import PrefectSecret
from prefect.engine.state import Failed, Success


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
