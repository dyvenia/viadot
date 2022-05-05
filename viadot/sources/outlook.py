from .base import Source
from O365 import Account
import pandas as pd
import datetime
from typing import Any, Dict, List
from ..config import local_config


class Outlook(Source):
    def __init__(
        self,
        mailbox_name: str,
        start_date: str = None,
        end_date: str = None,
        credentials: Dict[str, Any] = None,
        extension_file: str = ".csv",
        limit: int = 10000,
        request_retries: int = 10,
        # token_backend: str = "token_backend.txt",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """Outlook connector build for fetching Outlook API source.

        Args:
            mailbox_name (str): Mailbox name.
            start_date (str, optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.
            credentials (Dict[str, Any], optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Application.
            Defaults to None.
            extension_file (str, optional): Output file extension - to allow selection of .csv for data which is not easy
            to handle with parquet. Defaults to ".csv".
            limit (int, optional): Number of fetched top messages. Defaults to 10000.
        """
        super().__init__(*args, **kwargs)

        try:
            DEFAULT_CREDENTIALS = local_config["OUTLOOK"]
        except KeyError:
            DEFAULT_CREDENTIALS = None

        # self.token_backend = token_backend
        self.request_retries = request_retries
        self.credentials = credentials or DEFAULT_CREDENTIALS
        self.extension_file = extension_file
        self.mailbox_name = mailbox_name
        self.start_date = start_date
        self.end_date = end_date
        self.account = Account(
            (self.credentials["client_id"], self.credentials["client_secret"]),
            auth_flow_type="credentials",
            tenant_id=self.credentials["tenant_id"],
            main_resource=self.mailbox_name,
            request_retries=self.request_retries,
            # token_backend=self.token_backend,
        )
        if self.account.authenticate():
            print(f"{self.mailbox_name} Authenticated!")
        else:
            print(f"{self.mailbox_name} NOT Authenticated!")

        self.mailbox_obj = self.account.mailbox()
        self.mailbox_messages = self.mailbox_obj.get_messages(limit)
        super().__init__(*args, credentials=self.credentials, **kwargs)

    def to_df(self):
        date_range_end_time = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")
        date_range_start_time = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
        data = []
        # data_list_outbox, data_list_inbox = [], []

        while True:
            try:
                message = next(self.mailbox_messages)
                received_time = message.received
                date_time_str = str(received_time)
                dd = date_time_str[0:19]
                date_obj = datetime.datetime.strptime(dd, "%Y-%m-%d %H:%M:%S")
                if date_obj < date_range_start_time or date_obj > date_range_end_time:
                    continue
                else:
                    fetched = message.to_api_data()
                    try:
                        sender_mail = fetched["from"]["emailAddress"]["address"]
                        reciver_list = fetched.get("toRecipients")
                        recivers = ""
                        if reciver_list is not None:
                            recivers = ", ".join(
                                r["emailAddress"]["address"] for r in reciver_list
                            )
                        else:
                            recivers = ""
                        categories = ""
                        if message.categories:
                            categories = ", ".join(c for c in message.categories)

                        row = {
                            "subject": fetched.get("subject"),
                            "conversation ID": fetched.get("conversationId"),
                            "conversation index": message.conversation_index,
                            "categories": categories,
                            "sender": sender_mail,
                            "recivers": recivers,
                            "read": fetched.get("isRead"),
                            "received time": fetched.get("receivedDateTime"),
                        }
                        row["mail adress"] = (
                            self.mailbox_name.split("@")[0]
                            .replace(".", "_")
                            .replace("-", "_")
                        )

                        if sender_mail == self.mailbox_name:
                            row["Inbox"] = False
                        # data_list_outbox.append(row)
                        else:
                            row["Inbox"] = True
                        # data_list_inbox.append(row)

                        data.append(row)
                    except KeyError:
                        print(f"KeyError - nie ma w:")
            except StopIteration:
                break
        df = pd.DataFrame(data=data)

        # df_inbox = pd.DataFrame(data=data_list_inbox)
        # df_outbox = pd.DataFrame(data=data_list_outbox)

        return df  # df_inbox, df_outbox

    def to_csv(self):
        df_inbox, df_outbox = self.to_df()
        file_name = self.mailbox_name.split("@")[0].replace(".", "_").replace("-", "_")
        df_inbox.to_csv(f"{file_name}_Inbox{self.extension_file}", index=False)
        df_outbox.to_csv(f"{file_name}_Outbox{self.extension_file}", index=False)
