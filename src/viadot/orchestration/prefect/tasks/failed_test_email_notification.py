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


def find_model(compiled_code: str | None) -> str | None:
    """Extract a test model name from a string from a pandas Series.

    This function searches for a pattern in each element of the pandas Series
    from the "compiled_code" column.It handles strings with extra characters
    like new line characters etc.

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


def extract_model_ownership(manifest_file_path: str) -> pd.DataFrame:
    """Extract model ownership information from a dbt manifest.json file."""
    with Path(manifest_file_path).open() as file:
        manifest = json.load(file)

    result = []
    for _, node in manifest["nodes"].items():
        if node.get("resource_type") != "model":
            continue

        owners = node.get("config", {}).get("meta", {}).get("owners", [])
        technical_owner = next(
            (o["email"] for o in owners if o.get("type") == "Technical Owner"), None
        )

        result.append(
            {
                "model": node["name"],
                "schema": node["schema"],
                "database": node["database"],
                "technical_owner": technical_owner,
            }
        )

    df = pd.DataFrame(result)
    return df.drop_duplicates(subset=["model", "schema", "technical_owner"])


def get_owner_emails(
    failed_model: str, failed_schema: str, df: pd.DataFrame
) -> list[str]:
    """Get owner emails based on failed model and schema."""
    model_emails = (
        df.loc[df["model"] == failed_model, "technical_owner"]
        .dropna()
        .unique()
        .tolist()
    )
    if model_emails:
        return model_emails
    return (
        df.loc[df["schema"] == failed_schema, "technical_owner"]
        .dropna()
        .unique()
        .tolist()
    )


def enrich_with_owners(failed_tests: list, owners_df: pd.DataFrame) -> list:
    """Separate function responsible for enriching data with owners."""
    for test in failed_tests:
        test["owners"] = get_owner_emails(test["model"], test["schema"], owners_df)
    return failed_tests


def convert_json(file_path: str, test_types: tuple[str, ...]) -> list:
    """Convert DBT test results from JSON to a list of failed tests."""
    with Path(file_path).open() as file:
        data = json.load(file)

    df = pd.json_normalize(
        data,
        meta=[["metadata", "generated_at"]],
        record_path="results",
    )
    if df.empty:
        return []
    test_identifier = "^test\\."
    contains_test = df["unique_id"].str.match(test_identifier)
    if not contains_test.any():
        return []

    if not contains_test.all():
        df = df[contains_test].copy()

    df_failed = df[df["status"].isin(["error", "fail"])].copy()
    df_failed["schema"] = df_failed["compiled_code"].apply(find_schema)
    df_failed["model"] = df_failed["compiled_code"].apply(find_model)
    df_failed["column"] = df_failed["unique_id"].apply(find_column)
    df_failed["test_type"] = df_failed["unique_id"].apply(
        lambda x: find_test(x, test_types)
    )

    if df_failed.empty:
        return []

    return (
        df_failed[
            [
                "status",
                "model",
                "column",
                "test_type",
                "failures",
                "message",
                "schema",
                "unique_id",
            ]
        ]
        .rename(columns={"unique_id": "test_code"})
        .to_dict(orient="records")
    )


def parse_subject_from_message(message: str) -> str:
    """Extract model name from dbt error message as email subject."""
    match = re.search(r"test\s+(\S+)\s+\(", message)
    if match:
        return f"DBT Test Alert: {match.group(1)}"
    return "DBT Test Alert"


def dataframe_to_email_html(df: pd.DataFrame) -> str:
    """Convert DataFrame to a styled single-row HTML table for email."""
    styles = {
        "table": "border-collapse: collapse; font-family: Arial, sans-serif; font-size: 13px; width: 100%; border: 1px solid #000000;",
        "th": "background-color: #2c3e50; color: #ffffff; padding: 8px 12px; text-align: left; white-space: nowrap; border: 1px solid #000000;",
        "td": "padding: 8px 12px; border: 1px solid #000000; vertical-align: top;",
        "tr_even": "background-color: #f8f9fa;",
    }

    headers = "".join(f'<th style="{styles["th"]}">{col}</th>' for col in df.columns)

    rows = ""
    for i, (_, row) in enumerate(df.iterrows()):
        bg = f' style="{styles["tr_even"]}"' if i % 2 == 0 else ""
        cells = "".join(
            f'<td style="{styles["td"]}">{val if pd.notna(val) else "N/A"}</td>'
            for val in row
        )
        rows += f"<tr{bg}>{cells}</tr>"

    return f'<table style="{styles["table"]}"><thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table>'


def send_test_failure_notification(
    failed_test: pd.DataFrame,
    sender: str,
    server: smtplib.SMTP,
    default_recipients: list[str],
) -> None:
    """Send an email notification for a failed DBT test."""
    columns_to_skip = {"owners"}
    schema_name = (
        failed_test["schema"].iloc[0]
        if not pd.isna(failed_test["schema"].iloc[0])
        else "N/A"
    )
    model_name = (
        failed_test["model"].iloc[0]
        if not pd.isna(failed_test["model"].iloc[0])
        else "N/A"
    )
    column_name = (
        failed_test["column"].iloc[0]
        if not pd.isna(failed_test["column"].iloc[0])
        else "N/A"
    )
    owners = failed_test["owners"].explode().dropna()
    recipients = owners[owners.str.strip() != ""].unique().tolist() + default_recipients
    recipients_str = ", ".join(recipients)

    if any(v == "N/A" for v in [schema_name, column_name, model_name]):
        subject = parse_subject_from_message(failed_test["message"].iloc[0])
    else:
        subject = f"DBT Test Alert: {schema_name} - {model_name}"

    table_html = dataframe_to_email_html(failed_test.drop(columns=columns_to_skip))

    body_html = f"""
    <html>
    <body>
        <h2>DBT Test Failure Notification</h2>
        <p><strong>Recipients:</strong> {recipients_str}</p>
        {table_html}
    </body>
    </html>
    """

    msg = MIMEMultipart("mixed")
    msg["From"] = sender
    msg["To"] = recipients_str
    msg["Subject"] = subject
    msg.attach(MIMEText(body_html, "html"))
    server.sendmail(sender, recipients, msg.as_string())


@task(name="dbt-test-failure-notifier", cache_policy=None)
def dbt_test_failure_notifier(
    results_file_path: str,
    manifest_file_path: str,
    default_recipients: list[str],
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
    if not Path(results_file_path).exists():
        logger.warning(f"File {results_file_path} does not exist.")
        return

    failed_tests = convert_json(results_file_path, test_types)  # type: ignore
    if not failed_tests:
        logger.info("No failed tests — skipping notifications.")
        return
    owners_df = extract_model_ownership(manifest_file_path)
    failed_tests = enrich_with_owners(failed_tests, owners_df)

    df_failed_tests = pd.DataFrame(failed_tests)
    dfs_list = [group for _, group in df_failed_tests.groupby("model")]

    if smtp_config is None:
        smtp_config = SmtpConfig(
            sender=Secret.load("smtp-sender").get(),
            password=Secret.load("smtp-password").get(),
        )

    with smtplib.SMTP(smtp_config.host, smtp_config.port) as server:
        server.starttls()
        server.login(smtp_config.sender, smtp_config.password)
        sent = 0
        for single_model_tests in dfs_list:
            try:
                send_test_failure_notification(
                    single_model_tests, smtp_config.sender, server, default_recipients
                )
                sent += 1
            except smtplib.SMTPException:
                logger.exception(
                    f"Failed to send for {single_model_tests['model'].iloc[0]}"
                )
            except Exception:
                logger.exception(
                    f"Unexpected error for {single_model_tests['model'].iloc[0]}"
                )
        logger.info(f"Sent {sent}/{len(failed_tests)} failure notification(s).")
