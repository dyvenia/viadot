import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from O365 import Account
from O365.mailbox import MailBox
from pydantic import BaseModel

from ..config import get_source_credentials
from ..exceptions import CredentialError
from .base import Source


class OutlookCredentials(BaseModel):
    client_id: str
    client_secret: str
    tenant_id: str
    mail_example: str


class Outlook(Source):
    utc = timezone.utc

    def __init__(
        self,
        mailbox_name: str,
        request_retries: int = 10,
        credentials: Dict[str, Any] = None,
        config_key: str = "outlook",
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Outlook connector build for fetching Outlook API source.
        Data are fetched from start to end date range. If start or end date are not provided
        then flow fetched data from yestarday by default.

        Args:
            mailbox_name (str): Mailbox name.
            request_retries (int, optional): How many times retries to authorizate. Defaults to 10.
            credentials (Dict[str, Any], optional): The dictionary with outlook credentials. Default to None.
            config_key (str, optional): The key in the viadot config holding relevant credentials. Defaults to "outlook".
        """

        credentials = credentials or get_source_credentials(config_key)

        if credentials is None or not isinstance(credentials, dict):
            raise CredentialError("Missing credentials.")
        validated_creds = dict(OutlookCredentials(**credentials))

        # request_retries = request_retries
        self.mailbox_name = mailbox_name

        OutlookCredentials(**credentials)
        self.account = Account(
            (credentials["client_id"], credentials["client_secret"]),
            auth_flow_type="credentials",
            tenant_id=credentials["tenant_id"],
            main_resource=self.mailbox_name,
            request_retries=request_retries,
        )

        logging.basicConfig()
        super().__init__(*args, credentials=validated_creds, **kwargs)
        self.logger.setLevel(logging.INFO)

        if self.account.authenticate():
            self.logger.info(f"{self.mailbox_name} Authenticated!")
        else:
            self.logger.info(f"{self.mailbox_name} NOT Authenticated!")

        self.mailbox_obj = self.account.mailbox()

    @staticmethod
    def _get_subfolders(
        folder_structure: dict, folder: MailBox, key_concat: str = ""
    ) -> Dict[str, List]:
        """To retrieve all the subfolder in a MailBox folder.

        Args:
            folder_structure (dict): Dictionary where to save the data.
            folder (MailBox): The MailBox folder from where to extract the subfolders.
            key_concat (str, optional) Previous Mailbox folder structure to add to
                the actual subfolder. Defaults to "".

        Returns:
            Dict[str, List]: `folder_structure` dictionary is returned once it is updated.
        """
        if key_concat:
            tmp_key = key_concat.split("|")
            key_concat = key_concat.replace(f"|{tmp_key[-1]}", "")

        for subfolder in folder.get_folders():
            if subfolder:
                folder_structure.update(
                    {
                        "|".join([key_concat, folder.name, subfolder.name]).lstrip(
                            "|"
                        ): subfolder
                    }
                )

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
        while_dict_folders = {"key": "value"}
        while len(while_dict_folders) != 0:
            while_dict_folders = {}
            for key, value in list(dict_folders.items()):
                tmp_dict_folders = self._get_subfolders({}, value, key_concat=key)
                if tmp_dict_folders:
                    final_dict_folders.update(tmp_dict_folders)
                    while_dict_folders.update(tmp_dict_folders)

            dict_folders = while_dict_folders.copy()

        return final_dict_folders

    def _get_messages_from_mailbox(self, dict_folder: dict) -> list:
        """To retrieve all messages from all the mailboxes passed in the dictionary.

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
                        for reciver in recivers_list:
                            add_string = f", {reciver['emailAddress']['address']}"
                            if sum(list(map(len, [recivers, add_string]))) >= 8000:
                                break
                            else:
                                recivers += add_string

                    categories = " "
                    if message.categories is not None:
                        categories = ", ".join(
                            categories for categories in message.categories
                        )
                    conversation_index = " "

                    if message.conversation_index is not None:
                        conversation_index = message.conversation_index
                    if isinstance(message.subject, str):
                        subject = message.subject.replace("\t", " ")
                    else:
                        subject = message.subject

                    row = {
                        "(sub)folder": value.name,
                        "conversation ID": fetched.get("conversationId"),
                        "conversation index": conversation_index,
                        "categories": categories,
                        "sender": sender_mail,
                        "subject": subject,
                        "recivers": recivers.strip(", "),
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
            self.logger.info(f"folder: {key.ljust(76, '-')}  messages: {count}")

        return data

    def get_all_mails_to_df(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 10000,
    ) -> pd.DataFrame:
        """Download all the messages stored in a MailBox folder and subfolders.

        Args:
            start_date (Optional[str], optional): A filtering start date parameter e.g. "2022-01-01". Defaults to None.
            end_date (Optional[str], optional): A filtering end date parameter e.g. "2022-01-02". Defaults to None.
            limit (int, optional): Number of fetched top messages. Defaults to 10000.

        Returns:
            pd.DataFrame: All messages are stored in a pandas framwork.
        """
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
        self.limit = limit

        final_dict_folders = self._get_all_folders(self.mailbox_obj)

        data = self._get_messages_from_mailbox(final_dict_folders)

        df = pd.DataFrame(data=data)

        if df.empty:
            self._handle_if_empty("fail")

        return df

    def to_csv(self, df: pd.DataFrame) -> None:
        file_name = self.mailbox_name.split("@")[0].replace(".", "_").replace("-", "_")
        df.to_csv(f"{file_name}.csv", index=False)
