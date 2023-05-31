import pytz
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
from O365 import Account
from O365.mailbox import MailBox
from pydantic import BaseModel

from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.config import get_source_credentials


class OutlookCredentials(BaseModel):
    client_id: str
    client_secret: str
    tenant_id: str
    mail_example: str


class Outlook(Source):
    utc = pytz.UTC

    def __init__(
        self,
        mailbox_name: str,
        start_date: str = None,
        end_date: str = None,
        limit: int = 10000,
        request_retries: int = 10,
        credentials: Dict[str, Any] = None,
        credentials_secret: str = "outlook",
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
            limit (int, optional): Number of fetched top messages. Defaults to 10000.
            request_retries (int, optional): How many times retries to authorizate. Defaults to 10.
            credentials (Dict[str, Any], optional): The dictionary with outlook credentials. Default to None.
            credentials_secret (str, optional): The name of the Azure Key Vault secret containing a dictionary with
                ACCOUNT_NAME and Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Application.
                Defaults to None.
        """

        credentials = credentials or get_source_credentials(credentials_secret) or {}

        if credentials is None or not isinstance(credentials, dict):
            raise CredentialError("Please specify the credentials.")

        request_retries = request_retries
        self.mailbox_name = mailbox_name

        self.start_date = start_date
        self.end_date = end_date
        if self.start_date is not None and self.end_date is not None:
            self.date_range_end_time = datetime.strptime(self.end_date, "%Y-%m-%d")
            self.date_range_start_time = datetime.strptime(self.start_date, "%Y-%m-%d")
        else:
            self.date_range_start_time = date.today() - timedelta(days=1)
            self.date_range_end_time = date.today()

            min_time = datetime.min.time()
            self.date_range_end_time = datetime.combine(
                self.date_range_end_time, min_time
            )
            self.date_range_start_time = datetime.combine(
                self.date_range_start_time, min_time
            )

        OutlookCredentials(**credentials)
        self.account = Account(
            (credentials["client_id"], credentials["client_secret"]),
            auth_flow_type="credentials",
            tenant_id=credentials["tenant_id"],
            main_resource=self.mailbox_name,
            request_retries=request_retries,
        )

        logging.basicConfig()
        super().__init__(*args, credentials=credentials, **kwargs)
        self.logger.setLevel(logging.INFO)

        if self.account.authenticate():
            self.logger.info(f"{self.mailbox_name} Authenticated!")
        else:
            self.logger.info(f"{self.mailbox_name} NOT Authenticated!")

        self.mailbox_obj = self.account.mailbox()
        self.limit = limit

    @staticmethod
    def _get_subfolders(folder_structure: dict, folder: MailBox) -> Dict[str, List]:
        """To retrieve all the subfolder in a MailBox folder.

        Args:
            folder_structure (dict): Dictionary where to save the data.
            folder (MailBox): The MailBox folder from where to extract the subfolders.

        Returns:
            Dict[str, List]: `folder_structure` dictionary is returned once it is updated.
        """
        for subfolder in folder.get_folders():
            if subfolder:
                folder_structure.update({f"{folder.name}|{subfolder.name}": subfolder})

        if folder_structure:
            return folder_structure

    def _get_all_folders(self, mailbox: MailBox) -> dict:
        """To retrieve all folders from a Mailbox object.

        Args:
            mailbox (MailBox): Outlook Mailbox object from where to extract all folder structure.

        Returns:
            dict: Every single folder and subfolder is returned as "parent (sub)folder|(sub)folder": Mailbox.
        """
        dict_folders = self._get_subfolders({}, mailbox)
        final_dict_folders = dict_folders.copy()

        # loop to get all subfolders
        while True:
            while_dict_folders = {}
            for key, value in list(dict_folders.items()):
                tmp_dict_folders = self._get_subfolders({}, value)
                if tmp_dict_folders:
                    final_dict_folders.update(tmp_dict_folders)
                    while_dict_folders.update(tmp_dict_folders)

            dict_folders = while_dict_folders.copy()

            if len(while_dict_folders) == 0:
                break

        return final_dict_folders

    def _get_messages_from_mailbox(self, dict_folder: dict) -> list:
        """to retrieve all messages from all the mailboxes passed in the dictionary.

        Args:
            dict_folder (dict): Mailboxes dictionary holder, with the following structure:
                "parent (sub)folder|(sub)folder": Mailbox.

        Returns:
            list: A list with all messages from all Mailboxes.
        """
        data = []
        for key, value in list(dict_folder.items()):
            count = 0
            for message in value.get_messages(limit=self.limit):
                received_time = message.received
                date_obj = datetime.fromisoformat(str(received_time))
                if (
                    self.date_range_start_time.replace(tzinfo=self.utc)
                    < date_obj
                    < self.date_range_end_time.replace(tzinfo=self.utc)
                ):
                    count += 1
                    fetched = message.to_api_data()
                    sender_mail = fetched.get("from", None)
                    if sender_mail is not None:
                        sender_mail = fetched["from"]["emailAddress"]["address"]
                    recivers_list = fetched.get("toRecipients")
                    recivers = " "
                    if recivers_list is not None:
                        recivers = ", ".join(
                            reciver["emailAddress"]["address"]
                            for reciver in recivers_list
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
                        "(sub)folder": value.name,
                        "conversation ID": fetched.get("conversationId"),
                        "conversation index": conversation_index,
                        "categories": categories,
                        "sender": sender_mail,
                        "subject": message.subject,
                        "recivers": recivers,
                        "received_time": fetched.get("receivedDateTime"),
                        "mail_adress": self.mailbox_name.split("@")[0]
                        .replace(".", "_")
                        .replace("-", "_"),
                    }
                    if sender_mail == self.mailbox_name:
                        row["Inbox"] = False
                    else:
                        row["Inbox"] = True

                    data.append(row)
            self.logger.info(f"folder: {key.center(56, '-')}  messages: {count}")

        return data

    def get_all_mails_to_df(self) -> pd.DataFrame:
        """Download all the messages stored in a MailBox folder and subfolders.

        Returns:
            pd.DataFrame: All messages are stored in a pandas framwork.
        """
        final_dict_folders = self._get_all_folders(self.mailbox_obj)

        data = self._get_messages_from_mailbox(final_dict_folders)

        df = pd.DataFrame(data=data)

        return df

    def to_csv(self, df: pd.DataFrame) -> None:
        file_name = self.mailbox_name.split("@")[0].replace(".", "_").replace("-", "_")
        df.to_csv(f"{file_name}.csv", index=False)
