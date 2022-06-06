from faker import Faker
import pandas as pd
from pydantic import BaseModel, Field
from datetime import datetime
import random

fake = Faker()


class Contact(BaseModel):
    Id: str = Field(default_factory=lambda: i)
    AccountId: str = Field(default_factory=lambda: random.randint(1, account_nrows))
    FirstName: str = Field(default_factory=fake.first_name)
    LastName: str = Field(default_factory=fake.last_name)
    ContactEMail: str = Field(default_factory=fake.email)
    MailingCity: str = Field(default_factory=fake.city)
    Country: str = Field(default_factory=fake.country)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Account(BaseModel):
    id: str = Field(default_factory=lambda: i)
    name: str = Field(default_factory=fake.company)
    email: str = Field(default_factory=fake.email)
    mobile: str = Field(default_factory=fake.phone_number)
    country: str = Field(default_factory=fake.country)
    city: str = Field(default_factory=fake.city)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


account_nrows = 100
accounts = []
for i in range(1, account_nrows + 1):
    accounts.append(Account().dict())

contact_nrows = 1000
contacts = []
for i in range(1, contact_nrows + 1):
    contacts.append(Contact(Id=i).dict())


accounts_df_pandas = pd.DataFrame(accounts)
accounts_df_spark = spark.createDataFrame(accounts_df_pandas)
accounts_df_spark.write.saveAsTable("raw.c4c_account")

contacts_df_pandas = pd.DataFrame(contacts)
contacts_df_spark = spark.createDataFrame(contacts_df_pandas)
contacts_df_spark.write.saveAsTable("raw.c4c_contact")
