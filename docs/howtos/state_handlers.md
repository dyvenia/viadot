## Example usage of state handlers in flows

The important features of running flows are notifications and state handlers.
Prefect delivers such a solution. It is possible to be informed when a flow state changes.
Below you can find an example of using them.

### custom_mail_state_handler

```
from viadot.task_utils import custom_mail_state_handler

from_email = 'notifications@dyvenia.com'
to_emails = ['notifications@dyvenia.com', 'person1@dyvenia.com']

mail_handler = custom_mail_state_handler(
                    from_email=from_email,
                    to_emails=to_emails 
                    )

flow = ASELiteToADLS(
"ASELiteToADLS - test",
query = query,
sqldb_credentials_secret={sqldb credentials}
vault_name={key vault name},
file_path = {example file path},
to_path ="raw/test/example.csv",
sp_credentials_secret={sp_credentials_secret},
remove_special_characters=True,
state_handlers=[mail_handler]
)
flow.run()

```


::: viadot.task_utils.custom_mail_state_handler


### slack_notifier 

By default Prefect provide slack notifications.

For more read [Prefect Flow documentation](https://docs.prefect.io/core/advanced_tutorials/slack-notifications.html)

```
from prefect.utilities.notifications import slack_notifier

slack_handler = slack_notifier(only_states=[Failed])

flow = ASELiteToADLS(
"ASELiteToADLS - test",
query = query,
sqldb_credentials_secret={sqldb credentials}
vault_name={key vault name},
file_path = {example file path},
to_path ="raw/test/example.csv",
sp_credentials_secret={sp_credentials_secret},
remove_special_characters=True,
state_handlers=[slack_handler]
)
flow.run()

```