import random
from datetime import datetime

import pandas as pd
from faker import Faker
from prefect import flow, task
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession
from tasks import dbt_task, datahub_ingest_task

fake = Faker()

DBT_PROJECT_NAME = "databricks"


def gen_fake_contact_data(
    account_nrows: int, contact_nrows: int = 1000
) -> pd.DataFrame:
    class Contact(BaseModel):
        Id: str = Field(default_factory=lambda: i)
        AccountId: str = Field(default_factory=lambda: random.randint(1, account_nrows))
        FirstName: str = Field(default_factory=fake.first_name)
        LastName: str = Field(default_factory=fake.last_name)
        ContactEMail: str = Field(default_factory=fake.email)
        MailingCity: str = Field(default_factory=fake.city)
        Country: str = Field(default_factory=fake.country)
        updated_at: datetime = Field(default_factory=datetime.utcnow)

    contacts = []
    for i in range(1, contact_nrows + 1):
        contacts.append(Contact(Id=i).dict())
    df = pd.DataFrame(contacts)
    return df


def gen_fake_account_data(account_nrows: int = 100) -> pd.DataFrame:
    class Account(BaseModel):
        id: str = Field(default_factory=lambda: i)
        name: str = Field(default_factory=fake.company)
        email: str = Field(default_factory=fake.email)
        mobile: str = Field(default_factory=fake.phone_number)
        country: str = Field(default_factory=fake.country)
        city: str = Field(default_factory=fake.city)
        updated_at: datetime = Field(default_factory=datetime.utcnow)

    accounts = []
    for i in range(1, account_nrows + 1):
        accounts.append(Account().dict())
    df = pd.DataFrame(accounts)
    return df


@task(retries=3, retry_delay_seconds=3)
def drop_existing_data():
    spark = SparkSession.builder.appName("viadot").getOrCreate()

    spark.sql("drop table if exists staging.c4c_account")
    spark.sql("drop table if exists staging.c4c_contact")


@task(retries=3, retry_delay_seconds=3)
def write_fake_data():

    spark = SparkSession.builder.appName("viadot").getOrCreate()

    account_nrows = 100

    accounts_df_pandas = gen_fake_account_data(account_nrows)
    accounts_df_spark = spark.createDataFrame(accounts_df_pandas)
    accounts_df_spark.write.saveAsTable("staging.c4c_account")

    contacts_df_pandas = gen_fake_contact_data(account_nrows=account_nrows)
    contacts_df_spark = spark.createDataFrame(contacts_df_pandas)
    contacts_df_spark.write.saveAsTable("staging.c4c_contact")


@task
def print_task(text: str):
    print(text)


@flow(
    name="Test Data Platform flow",
    description="A test flow to test Data Platform functionalities",
)
def main(to_print: str = None):
    p = print_task(to_print)
    prepare_env = drop_existing_data()
    fake_data = write_fake_data(wait_for=[prepare_env])
    run_dbt = dbt_task(
        dbt_project_name=DBT_PROJECT_NAME, command="run", wait_for=[fake_data]
    )
    test_dbt = dbt_task(
        dbt_project_name=DBT_PROJECT_NAME, command="test", wait_for=[run_dbt]
    )
    gen_dbt_metadata = dbt_task(
        dbt_project_name=DBT_PROJECT_NAME, command="docs generate", wait_for=[test_dbt]
    )
    upload_dbt_metadata = datahub_ingest_task(
        recipe_path=f"recipes/dbt_{DBT_PROJECT_NAME}.yml", wait_for=[gen_dbt_metadata]
    )
    return upload_dbt_metadata


if __name__ == "__main__":
    state = main()
