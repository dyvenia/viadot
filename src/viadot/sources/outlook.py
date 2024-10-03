"""Module for fetching data from the Outlook API."""

from datetime import date, datetime, timedelta, timezone
from typing import Any

from O365 import Account
from O365.mailbox import MailBox
import pandas as pd
from pydantic import BaseModel

from viadot.config import get_source_credentials
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class OutlookCredentials(BaseModel):
    """Checking for values in Outlook credentials dictionary.

    Two key values are held in the Outlook connector:
        - client_id:
        - client_secret:
        - tenant_id:

    Args:
        BaseModel (pydantic.main.ModelMetaclass): A base class for creating
            Pydantic models.
    """

    client_id: str
    client_secret: str
    tenant_id: str


class Outlook(Source):
    """Class implementing the Outlook API.

    Documentation for this API is available at:
        https://o365.github.io/python-o365/latest/getting_started.html.
    """

    UTC = timezone.utc

    def __init__(
        self,
        *args: list[Any],
        credentials: dict[str, Any] | None = None,
        config_key: str = "outlook",
        **kwargs: dict[str, Any],
    ):
        """Outlook connector build for fetching Outlook API source.

        Data are fetched from start to end date range. If start or end date are not
        provided then flow fetched data from yesterday by default.

        Args:
            credentials (Optional[OutlookCredentials], optional): Outlook credentials.
                Defaults to None
            config_key (str, optional): The key in the viadot config holding relevant
                credentials. Defaults to "outlook".

        Examples:
            outlook = Outlook(
                config_key=config_key,
            )
            outlook.api_connection(
                mailbox_name=mailbox_name,
                request_retries=request_retries,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                address_limit=address_limit,
                outbox_list=outbox_list,
            )
            data_frame = outlook.to_df()

        Raises:
            CredentialError: If credentials are not provided in local_config or
                directly as a parameter.
        """
        raw_creds = credentials or get_source_credentials(config_key)
        validated_creds = dict(OutlookCredentials(**raw_creds))

        super().__init__(*args, credentials=validated_creds, **kwargs)

    @staticmethod
    def _get_subfolders(
        folder_structure: dict,
        folder: MailBox,
        key_concat: str = "",
    ) -> dict[str, list] | None:
        """Retrieve all the subfolder in a MailBox folder.

        Args:
            folder_structure (dict): Dictionary where to save the data.
            folder (MailBox): The MailBox folder from where to extract the subfolders.
            key_concat (str, optional) Previous Mailbox folder structure to add to
                the actual subfolder. Defaults to "".

        Returns:
            Dict[str, List]: `folder_structure` dictionary is returned once
                it is updated.
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

        return None

    def _get_all_folders(
        self,
        mailbox: MailBox,
    ) -> dict:
        """To retrieve all folders from a Mailbox object.

        Args:
            mailbox (MailBox): Outlook Mailbox object from where to extract all
                folder structure.

        Returns:
            dict: Every single folder and subfolder is returned as
                "parent (sub)folder|(sub)folder": Mailbox.
        """
        dict_folders = self._get_subfolders({}, mailbox)
        final_dict_folders = dict_folders.copy()

        # Get all subfolders.
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

    # TODO: should be refactored.
    def _get_messages_from_mailbox(  # noqa: C901, PLR0912
        self,
        mailbox_name: str,
        dict_folder: dict,
        date_range_start_time: datetime,
        date_range_end_time: datetime,
        limit: int = 10000,
        address_limit: int = 8000,
        outbox_list: list[str] | None = None,
    ) -> list:
        """To retrieve all messages from all the mailboxes passed in the dictionary.

        Args:
            mailbox_name (str): Mailbox name.
            dict_folder (dict): Mailboxes dictionary holder, with the following
                structure: "parent (sub)folder|(sub)folder": Mailbox.
            date_range_start_time (datetime): Start date from where to stract data.
            date_range_end_time (datetime): End data up to where to stract data.
            limit (int, optional): Number of fetched top messages. Defaults to 10000.
            address_limit (int, optional): The maximum number of accepted characters in
                the sum of all email names. Defaults to 8000.
            outbox_list (List[str], optional): List of outbox folders to differenciate
                between Inboxes and Outboxes. Defaults to ["Sent Items"].

        Returns:
            list: A list with all messages from all Mailboxes.
        """
        if not outbox_list:
            outbox_list = ["Sent Items"]

        data = []
        for key, value in list(dict_folder.items()):
            count = 0
            for message in value.get_messages(limit=limit):
                received_time = message.received
                date_obj = datetime.fromisoformat(str(received_time))
                if (
                    date_range_start_time.replace(tzinfo=self.UTC)
                    < date_obj
                    < date_range_end_time.replace(tzinfo=self.UTC)
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
                            if (
                                sum(list(map(len, [recivers, add_string])))
                                >= address_limit
                            ):
                                break

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
                        "mail_adress": mailbox_name.split("@")[0]
                        .replace(".", "_")
                        .replace("-", "_"),
                    }
                    if any(x.lower() in key.lower() for x in outbox_list):
                        row["Inbox"] = False
                    else:
                        row["Inbox"] = True

                    data.append(row)

            if count > 0:
                self.logger.info(f"folder: {key.ljust(76, '-')}  messages: {count}")

        return data

    def api_connection(
        self,
        mailbox_name: str | None = None,
        request_retries: int = 10,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 10000,
        address_limit: int = 8000,
        outbox_list: list[str] | None = None,
    ) -> pd.DataFrame:
        """Download all the messages stored in a MailBox folder and subfolders.

        Args:
            mailbox_name (Optional[str], optional): Mailbox name. Defaults to None.
            request_retries (int, optional): How many times retries to authorizate.
                Defaults to 10.
            start_date (Optional[str], optional): A filtering start date parameter e.g.
                "2022-01-01". Defaults to None.
            end_date (Optional[str], optional): A filtering end date parameter e.g.
                "2022-01-02". Defaults to None.
            limit (int, optional): Number of fetched top messages. Defaults to 10000.
            address_limit (int, optional): The maximum number of accepted characters in
                the sum of all email names. Defaults to 8000.
            outbox_list (List[str], optional): List of outbox folders to differentiate
                between Inboxes and Outboxes. Defaults to ["Sent Items"].

        Returns:
            pd.DataFrame: All messages are stored in a pandas framework.
        """
        if not outbox_list:
            outbox_list = ["Sent Items"]
        account = Account(
            (self.credentials["client_id"], self.credentials["client_secret"]),
            auth_flow_type="credentials",
            tenant_id=self.credentials["tenant_id"],
            main_resource=mailbox_name,
            request_retries=request_retries,
        )

        if account.authenticate():
            self.logger.info(f"{mailbox_name} Authenticated!")
        else:
            msg = "Failed to authenticate."
            raise ValueError(msg)

        mailbox_obj = account.mailbox()

        if start_date is not None and end_date is not None:
            date_range_end_time = datetime.strptime(end_date, "%Y-%m-%d")
            date_range_start_time = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            date_range_start_time = date.today() - timedelta(days=1)
            date_range_end_time = date.today()

            min_time = datetime.min.time()
            date_range_end_time = datetime.combine(date_range_end_time, min_time)
            date_range_start_time = datetime.combine(date_range_start_time, min_time)

        final_dict_folders = self._get_all_folders(mailbox_obj)

        self.data = self._get_messages_from_mailbox(
            mailbox_name=mailbox_name,
            dict_folder=final_dict_folders,
            date_range_start_time=date_range_start_time,
            date_range_end_time=date_range_end_time,
            limit=limit,
            address_limit=address_limit,
            outbox_list=outbox_list,
        )

    @add_viadot_metadata_columns
    def to_df(
        self,
        if_empty: str = "warn",
    ) -> pd.DataFrame:
        """Generate a pandas DataFrame with the data.

        Args:
            if_empty (str, optional): What to do if a fetch produce no data.
                Defaults to "warn

        Returns:
            pd.Dataframe: The response data as a pandas DataFrame plus viadot metadata.
        """
        super().to_df(if_empty=if_empty)

        data_frame = pd.DataFrame(self.data)

        if data_frame.empty:
            self._handle_if_empty(
                if_empty="warn",
                message="No data was got from the Mail Box for those days",
            )
        else:
            self.logger.info("Successfully downloaded data from the Mindful API.")

        return data_frame
