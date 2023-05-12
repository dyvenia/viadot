import sys

import pytz
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Literal, Union

import pandas as pd
from O365 import Account
from O365.mailbox import MailBox

from viadot.config import local_config
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
import prefect


class Outlook(Source):
    utc = pytz.UTC

    def __init__(
        self,
        mailbox_name: str,
        start_date: str = None,
        end_date: str = None,
        credentials: Dict[str, Any] = None,
        limit: int = 10000,
        mailbox_folders: Literal[
            "sent", "inbox", "junk", "deleted", "drafts", "outbox", "archive"
        ] = ["sent", "inbox", "junk", "deleted", "drafts", "outbox", "archive"],
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
            mailbox_folders (Literal["sent", "inbox", "junk", "deleted", "drafts", "outbox", "archive"]):
                List of folders to select from the mailbox.  Defaults to ["sent", "inbox", "junk", "deleted", "drafts", "outbox", "archive"]
            request_retries (int, optional): How many times retries to authorizate. Defaults to 10.
        """

        self.logger = prefect.context.get("logger")

        self.credentials = credentials
        if self.credentials is None:
            raise CredentialError("You do not provide credentials!")

        self.request_retries = request_retries
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

        self.account = Account(
            (self.credentials["client_id"], self.credentials["client_secret"]),
            auth_flow_type="credentials",
            tenant_id=self.credentials["tenant_id"],
            main_resource=self.mailbox_name,
            request_retries=self.request_retries,
        )
        if self.account.authenticate():
            self.logger.info(f"{self.mailbox_name} Authenticated!")
        else:
            self.logger.info(f"{self.mailbox_name} NOT Authenticated!")

        self.mailbox_obj = self.account.mailbox()

        # print(type(self.mailbox_obj))
        # print(dir(self.mailbox_obj))
        # print()
        # l = []
        # for folder in self.mailbox_obj.get_folders():
        #     print(folder, type(folder))
        #     l.append(folder)
        #     for subfolder in folder.get_folders():
        #         if subfolder:
        #             print("\t", subfolder, type(subfolder))
        #             l.append(subfolder)
        #             for subsubfolder in subfolder.get_folders():
        #                 if subsubfolder:
        #                     print("\t\t", subsubfolder)
        #                     l.append(subsubfolder)
        #                     for subsubsubfolder in subsubfolder.get_folders():
        #                         if subsubsubfolder:
        #                             print("\t\t\t", subsubsubfolder)
        #                             l.append(subsubsubfolder)
        #                             for (
        #                                 subsubsubsubfolder
        #                             ) in subsubsubfolder.get_folders():
        #                                 if subsubsubsubfolder:
        #                                     print("\t\t\t\t", subsubsubsubfolder)
        #                                     l.append(subsubsubsubfolder)
        # print(len(l))
        # sys.exit()

        self.limit = limit

        super().__init__(*args, credentials=self.credentials, **kwargs)

    @staticmethod
    def _get_subfolders(folder_structure: dict, folder: MailBox) -> Dict[str, List]:
        """Retrieve all the subfolder in a MailBox folder.

        Args:
            folder_structure (dict): Dictionary where to save the data.
            folder (MailBox): The MailBox folder from where to extract the subfolders.

        Returns:
            Dict[str, List]: `folder_structure` dictionary is returned once it is updated.
        """
        for subfolder in folder.get_folders():
            if subfolder:
                # print(subfolder.name)
                folder_structure.update({f"{folder.name}|{subfolder.name}": subfolder})
                # list_of_folders.append(subfolder)
                # sys.exit()

        if folder_structure:
            return folder_structure

    def get_all_mails_to_df(self) -> pd.DataFrame:
        """Download all the messages stored in a MailBox folder and subfolders.

        Returns:
            pd.DataFrame: All messages are stored in a pandas framwork.
        """
        dict_folders = self._get_subfolders({}, self.mailbox_obj)

        final_dict_folders = dict_folders.copy()

        # loop to get all subfolders
        while True:
            while_dict_folders = {}
            for key, value in list(dict_folders.items()):
                # print(key)
                tmp_dict_folders = self._get_subfolders({}, value)
                if tmp_dict_folders:
                    # print(tmp_dict_folders)
                    # print(len(tmp_dict_folders))

                    final_dict_folders.update(tmp_dict_folders)
                    # print(len(final_dict_folders))

                    while_dict_folders.update(tmp_dict_folders)

            dict_folders = while_dict_folders.copy()

            if len(while_dict_folders) == 0:
                break

        data = []
        for key, value in list(final_dict_folders.items()):
            print(key)
            for message in value.get_messages():
                received_time = message.received
                date_obj = datetime.fromisoformat(str(received_time))
                if (
                    self.date_range_start_time.replace(tzinfo=self.utc)
                    < date_obj
                    < self.date_range_end_time.replace(tzinfo=self.utc)
                ):
                    fetched = message.to_api_data()
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
                        "conversation ID": fetched.get("conversationId"),
                        "conversation index": conversation_index,
                        "categories": categories,
                        "sender": sender_mail,
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

            # break

        df = pd.DataFrame(data=data)

        return df

    def to_df(self) -> pd.DataFrame:
        """Download Outlook data into a pandas DataFrame.

        Returns:
            pd.DataFrame: the DataFrame with time range.
        """
        data = []
        mailbox_generators_list = []
        for m in self.mailbox_folders:
            base_str = f"self.mailbox_obj.{m}_folder().get_messages({self.limit})"
            mailbox_generators_list.append(eval(base_str))

        for mailbox_generator in mailbox_generators_list:
            while True:
                try:
                    message = next(mailbox_generator)
                    received_time = message.received
                    date_time_str = str(received_time)
                    date_time_stripped = date_time_str[0:19]
                    date_obj = datetime.datetime.strptime(
                        date_time_stripped, "%Y-%m-%d %H:%M:%S"
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
                            self.logger.info("KeyError : " + str(e))
                except StopIteration:
                    break
        df = pd.DataFrame(data=data)

        return df

    def to_csv(self, df: pd.DataFrame) -> None:
        file_name = self.mailbox_name.split("@")[0].replace(".", "_").replace("-", "_")
        df.to_csv(f"{file_name}.csv", index=False)
