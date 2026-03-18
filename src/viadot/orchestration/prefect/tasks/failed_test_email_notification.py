import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from prefect import task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger


def get_smtp_config(sender_block: str, password_block: str) -> dict:
    return {
        "host": "smtp.gmail.com",
        "port": 587,
        "sender": Secret.load(sender_block).get(),
        "password": Secret.load(password_block).get(),
    }


def convert_json_to_df(file_path):
    with open(file_path) as file:
        data = json.load(file)

    df = pd.json_normalize(
        data,
        meta=[["metadata", "generated_at"]],
        record_path="results",
    )
    df_failed = df[df["status"].isin(["error", "fail"])]

    if df_failed.empty:
        return []
    
    return df_failed[["unique_id", "status", "message", "failures"]].to_dict(orient="records")


def send_test_failure_notification(test: dict, smtp_config: dict, recipient: str):

    subject = f"DBT Test Failed: {test['unique_id']}"
    body = f"""
    DBT Test Failure Notification
    
    Test:     {test['unique_id']}
    Status:   {test['status']}
    Message:  {test['message']}
    Failures: {test['failures']}
    """

    msg = MIMEMultipart()
    msg["From"] = smtp_config["sender"]
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(smtp_config["host"], smtp_config["port"]) as server:
        server.starttls()
        server.login(smtp_config["sender"], smtp_config["password"])
        server.sendmail(smtp_config["sender"], recipient, msg.as_string())


@task(name="dbt-test-failure-notifier")
def dbt_test_failure_notifier(file_path: str, 
    recipient: str, 
    smtp_sender_block: str = "smtp-sender",
    smtp_password_block: str = "smtp-password") -> None:

    logger = get_run_logger()

    failed_tests = convert_json_to_df(file_path)

    if not failed_tests:
        logger.info("No failed tests — skipping notifications.")
        return
    
    smtp_config = get_smtp_config(smtp_sender_block, smtp_password_block)

    for test in failed_tests:
        send_test_failure_notification(test, smtp_config, recipient)