from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import smtplib

import pandas as pd
from prefect import task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger


def get_smtp_config(sender_block: str, password_block: str) -> dict:
    """Retrieve SMTP configuration from Prefect Secrets."""
    return {
        "host": "smtp.gmail.com",
        "port": 587,
        "sender": Secret.load(sender_block).get(),
        "password": Secret.load(password_block).get(),
    }


def convert_json_to_df(file_path: str) -> list:
    """Convert DBT test results from JSON to a list of failed tests."""
    with open(file_path) as file:
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

    df_failed = df[df["status"].isin(["error", "fail"])]

    if df_failed.empty:
        return []

    return df_failed[["unique_id", "status", "message", "failures"]].to_dict(
        orient="records"
    )


def send_test_failure_notification(
    test: dict, smtp_config: dict, recipient: str, server: smtplib.SMTP
) -> None:
    """Send an email notification for a failed DBT test."""
    subject = f"DBT Test Failed: {test['unique_id']}"
    body = f"""
    DBT Test Failure Notification

    Test:     {test["unique_id"]}
    Status:   {test["status"]}
    Message:  {test["message"]}
    Failures: {test["failures"]}
    """

    msg = MIMEMultipart()
    msg["From"] = smtp_config["sender"]
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    server.sendmail(smtp_config["sender"], recipient, msg.as_string())


@task(name="dbt-test-failure-notifier", cache_policy=None)
def dbt_test_failure_notifier(
    file_path: str,
    recipient: str,
    smtp_sender_block: str = "smtp-sender",
    smtp_password_block: str = "smtp-password",  # noqa: S107
) -> None:
    """Prefect task to send email notifications for failed DBT tests."""
    logger = get_run_logger()
    failed_tests = convert_json_to_df(file_path)
    if not failed_tests:
        logger.info("No failed tests — skipping notifications.")
        return

    smtp_config = get_smtp_config(smtp_sender_block, smtp_password_block)
    with smtplib.SMTP(smtp_config["host"], smtp_config["port"]) as server:
        server.starttls()
        server.login(smtp_config["sender"], smtp_config["password"])
        sent = 0
        for test in failed_tests:
            try:
                send_test_failure_notification(test, smtp_config, recipient, server)
                sent += 1
            except smtplib.SMTPException as e:
                logger.exception(f"Failed to send for {test['unique_id']}: {e}")
            except Exception as e:
                logger.exception(f"Unexpected error for {test['unique_id']}: {e}")
        logger.info(f"Sent {sent}/{len(failed_tests)} failure notification(s).")
