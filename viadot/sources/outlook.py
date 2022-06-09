from .base import Source
from O365 import Account
import pandas as pd
import datetime
from typing import Any, Dict, List
from ..config import local_config
from ..exceptions import CredentialError


class Outlook(Source):
    def __init__(
        self,
        mailbox_name: str,
        start_date: str = None,
        end_date: str = None,
        credentials: Dict[str, Any] = None,
        limit: int = 10000,
        request_retries: int = 10,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Outlook connector build for fetching Outlook API source.
        Data are fetched from start to end date range. If start or end date are not provided
        then flow fetched data from yestarday by default.

        Args:
            mailbox_name (str): Mailbox name.
            start_date (str, optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (str, optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.
            credentials (Dict[str, Any], optional): The name of the Azure Key Vault secret containing a dictionary with
            ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Application.
            Defaults to None.
            limit (int, optional): Number of fetched top messages. Defaults to 10000.
        """
        try:
            DEFAULT_CREDENTIALS = local_config["OUTLOOK"]
        except KeyError:
            DEFAULT_CREDENTIALS = None
        self.credentials = credentials or DEFAULT_CREDENTIALS
        if self.credentials is None:
            raise CredentialError("You do not provide credentials!")

        self.request_retries = request_retries
        self.mailbox_name = mailbox_name
        self.start_date = start_date
        self.end_date = end_date
        if self.start_date is not None and self.end_date is not None:
            self.date_range_end_time = datetime.datetime.strptime(
                self.end_date, "%Y-%m-%d"
            )
            self.date_range_start_time = datetime.datetime.strptime(
                self.start_date, "%Y-%m-%d"
            )
        else:
            self.date_range_end_time = datetime.date.today() - datetime.timedelta(
                days=1
            )
            self.date_range_start_time = datetime.date.today() - datetime.timedelta(
                days=2
            )
            min_time = datetime.datetime.min.time()
            self.date_range_end_time = datetime.datetime.combine(
                self.date_range_end_time, min_time
            )
            self.date_range_start_time = datetime.datetime.combine(
                self.date_range_start_time, min_time
            )

        self.account = Account(
            (self.credentials["client_id"], self.credentials["client_secret"]),
            auth_flow_type="credentials",
            tenant_id=self.credentials["tenant_id"],
            main_resource=self.mailbox_name,
            request_retries=self.request_retries,
        )
        if self.account.authenticate():
            print(f"{self.mailbox_name} Authenticated!")
        else:
            print(f"{self.mailbox_name} NOT Authenticated!")

        self.mailbox_obj = self.account.mailbox()
        self.mailbox_messages = self.mailbox_obj.get_messages(limit)
        super().__init__(*args, credentials=self.credentials, **kwargs)

    def to_df(self) -> pd.DataFrame:
        """Download Outlook data into a pandas DataFrame.

        Returns:
            pd.DataFrame: the DataFrame with time range
        """
        data = []

        while True:
            try:
                message = next(self.mailbox_messages)
                received_time = message.received
                date_time_str = str(received_time)
                date_time_string = date_time_str[0:19]
                date_obj = datetime.datetime.strptime(
                    date_time_string, "%Y-%m-%d %H:%M:%S"
                )
                if (
                    date_obj < self.date_range_start_time
                    or date_obj > self.date_range_end_time
                ):
                    continue
                else:
                    fetched = message.to_api_data()
                    try:
                        sender_mail = fetched["from"]["emailAddress"]["address"]
                        reciver_list = fetched.get("toRecipients")
                        recivers = " "
                        if reciver_list is not None:
                            recivers = ", ".join(
                                reciver["emailAddress"]["address"]
                                for reciver in reciver_list
                            )

                        categories = " "
                        if message.categories is not None:
                            categories = ", ".join(
                                categories for categories in message.categories
                            )

                        conversation_index = " "
                        if message.conversation_index is not None:
                            conversation_index = message.conversation_index
                        row = {
                            "conversation ID": fetched.get("conversationId"),
                            "conversation index": conversation_index,
                            "categories": categories,
                            "sender": sender_mail,
                            "recivers": recivers,
                            "received_time": fetched.get("receivedDateTime"),
                        }

                        row["mail_adress"] = (
                            self.mailbox_name.split("@")[0]
                            .replace(".", "_")
                            .replace("-", "_")
                        )
                        if sender_mail == self.mailbox_name:
                            row["Inbox"] = False
                        else:
                            row["Inbox"] = True

                        data.append(row)
                    except KeyError as e:
                        print("KeyError : " + str(e))
            except StopIteration:
                break
        df = pd.DataFrame(data=data)

        return df

    def to_csv(self):
        df = self.to_df()
        file_name = self.mailbox_name.split("@")[0].replace(".", "_").replace("-", "_")
        df.to_csv(f"{file_name}.csv", index=False)
