"""Module for sending email notifications about failed dbt tests."""

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from pathlib import Path
import re
import smtplib

import pandas as pd
from prefect import task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger
from pydantic import BaseModel


class SmtpConfig(BaseModel):
    host: str = "smtp.gmail.com"
    port: int = 587
    sender: str
    password: str


def find_schema(compiled_code: str | None) -> str | None:
    """Extract a test schema name from a string from a pandas Series.

        This function searches for a pattern in each element of the pandas Series
        from the "compiled_code" column.It handles strings with extra characters
        like new line characters etc.

    Example:
        Extracts "intermediate" from string:
            select * from werfendb.intermediate.int_but0is where "partner_id is null
    Args:
        compiled_code (str | None):  SQL string (or None) to extract
        the schema name from

    Returns:
        str: A string containing the extracted patterns or None if not found.
    """
    if compiled_code is None:
        return None
    pattern = r'[\"a-zA-Z0-9_\"]\.\"([^"]+)\"\.'
    match = re.search(pattern, compiled_code)
    return match.group(1) if match else None


def find_table(compiled_code: str | None) -> str | None:
    """Extract a test table name from a string from a pandas Series.

    This function searches for a pattern in each element of the pandas Series
    from the "compiled_code" column.It handles strings with extra characters
    like new line characters etc.

    Example:
        Extracts "int_but0is" from string:
            select * from werfendb.intermediate.int_but0is where "partner_id is null
    Args:
        compiled_code (str | None): SQL string (or None) to extract
        the table name from

    Returns:
        str: A string containing the extracted patterns or None if not found.
    """
    if compiled_code is None:
        return None
    pattern = r"[\"a-zA-Z0-9_\"]\.+[\"a-zA-Z0-9_\"]+\".+\"([a-zA-Z0-9_]+?)\""
    match = re.search(pattern, compiled_code)
    return match.group(1) if match else None


def find_column(unique_id: str | None) -> str | None:
    """Extract a test column name from a string from a pandas Series.

    This function searches for a pattern in each element of the pandas Series
    from the "unique_id" column.

    Example:
        Extracts "partner_id" from string:
            "test.lakehouse.not_null_int_but0is__partner_id_.bb2a44d06f"
    Args:
        unique_id (str | None): unique_id to string (or None) to extract
        the column name from

    Returns:
        str: A string containing the extracted patterns or None if not found.
    """
    if unique_id is None:
        return None
    pattern = r".*?__([a-zA-Z0-9_]+?)(?=_?\.[a-zA-Z0-9]+$|__)"
    match = re.search(pattern, unique_id)
    return match.group(1) if match else None


def find_test(value: str | None, test_types: tuple[str, ...]) -> str | None:
    """Extract a specific test name from a string in pandas Series.

    This function searches for allowed test types in each element
        of the pandas Series(df column).

    Example:
        Search for test in the string below will return "not_null"
            "test.lakehouse.not_null_int_but0is__partner_id_.bb2a44d06f"
    Args:
        value (str | None): The string (or None) to search within.
        test_types: (tuple[str, ...]): A list of test types that can be discovered.

    Returns:
        str | None: The matched test type, or None if no match is found.
    """
    if value is None:
        return None
    for test_type in test_types:
        if test_type in value:
            return test_type
    return None


def convert_json_to_df(file_path: str, test_types: tuple[str, ...]) -> list:
    """Convert DBT test results from JSON to a list of failed tests."""
    with Path(file_path).open() as file:
        data = json.load(file)

    df = pd.json_normalize(
        data,
        meta=[["metadata", "generated_at"]],
        record_path="results",
    )
    test_identifier = "^test\\."
    contains_test = df["unique_id"].str.match(test_identifier)

    if not contains_test.any():
        return []

    if not contains_test.all():
        df = df[contains_test].copy()

    df_failed = df[df["status"].isin(["error", "fail"])].copy()

    df_failed["schema"] = df_failed["compiled_code"].apply(find_schema)
    df_failed["table"] = df_failed["compiled_code"].apply(find_table)
    df_failed["column"] = df_failed["unique_id"].apply(find_column)
    df_failed["test_type"] = df_failed["unique_id"].apply(
        lambda x: find_test(x, test_types)
    )

    if df_failed.empty:
        return []

    return df_failed[
        [
            "unique_id",
            "schema",
            "status",
            "message",
            "failures",
            "table",
            "column",
            "test_type",
        ]
    ].to_dict(orient="records")


def send_test_failure_notification(
    test: dict, sender: str, recipients: list[str], server: smtplib.SMTP
) -> None:
    """Send an email notification for a failed DBT test."""
    schema_name = test["schema"]
    column_name = test["column"] or "column not identified"
    subject = f"DBT Test Alert: {schema_name} - {column_name}"
    body = f"""
    DBT Test Failure Notification

    Test:     {test["unique_id"]}
    Schema:   {schema_name}
    Table:    {test["table"] or "N/A"}
    Column:   {column_name}
    Test Type: {test["test_type"] or "N/A"}
    Status:   {test["status"]}
    Message:  {test["message"]}
    Failures: {test["failures"] or "N/A"}
    """
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    server.sendmail(sender, recipients, msg.as_string())


@task(name="dbt-test-failure-notifier", cache_policy=None)
def dbt_test_failure_notifier(
    file_path: str,
    recipients: list[str],
    smtp_config: SmtpConfig | None = None,
    test_types: tuple[str, ...] = (
        "not_null",
        "unique",
        "accepted_values",
        "relationships",
    ),
) -> None:
    """Prefect task to send email notifications for failed DBT tests."""
    logger = get_run_logger()
    parent = Path(file_path).parent
    logger.warning(f"Files in {parent}:")
    for f in parent.iterdir():
        logger.warning(f"  {f.name}")

    if not Path(file_path).exists():
        logger.warning(f"File {file_path} does not exist.")
        return

    failed_tests = convert_json_to_df(file_path, test_types)  # type: ignore
    if not failed_tests:
        logger.info("No failed tests — skipping notifications.")
        return

    if smtp_config is None:
        smtp_config = SmtpConfig(
            sender=Secret.load("smtp-sender").get(),
            password=Secret.load("smtp-password").get(),
        )

    with smtplib.SMTP(smtp_config.host, smtp_config.port) as server:
        server.starttls()
        server.login(smtp_config.sender, smtp_config.password)
        sent = 0
        for test in failed_tests:
            try:
                send_test_failure_notification(
                    test, smtp_config.sender, recipients, server
                )
                sent += 1
            except smtplib.SMTPException:
                logger.exception(f"Failed to send for {test['unique_id']}")
            except Exception:
                logger.exception(f"Unexpected error for {test['unique_id']}")
        logger.info(f"Sent {sent}/{len(failed_tests)} failure notification(s).")
