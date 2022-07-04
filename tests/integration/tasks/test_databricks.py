import pyspark
from viadot.sources import Databricks
import pandas as pd

databricks = Databricks(env="QA")


def test_to_df():
    # Look up test_query table and check if the type and results returned are correct
    source_data = [
        {
            "Id": "wRACnHTeuw",
            "AccountId": "rWFuwDHxfGh",
            "Name": "Scott-Merritt",
            "FirstName": "Melody",
            "LastName": "Cook",
            "ContactEMail": "Melody.Cook@ScottMerritt.com",
            "MailingCity": "Elizabethfurt",
        },
        {
            "Id": "CFfTlqagNlpDu",
            "AccountId": "TtGjFbCTWJgdPT",
            "Name": "Mann-Warren",
            "FirstName": "Wayne",
            "LastName": "Morrison",
            "ContactEMail": "Wayne.Morrison@MannWarren.com",
            "MailingCity": "Kathrynmouth",
        },
        {
            "Id": "SNaTEOiGfPJ",
            "AccountId": "KxPqtfmCHA",
            "Name": "Peters, Perez and Bowman",
            "FirstName": "James",
            "LastName": "Ortiz",
            "ContactEMail": "James.Ortiz@PetersPerezandBowman.com",
            "MailingCity": "Michaelland",
        },
        {
            "Id": "yzdeNFYNPJWF",
            "AccountId": "wsmjsVQpKWUbA",
            "Name": "Estrada-Hayes",
            "FirstName": "Vanessa",
            "LastName": "Guzman",
            "ContactEMail": "Vanessa.Guzman@EstradaHayes.com",
            "MailingCity": "Barnesport",
        },
        {
            "Id": "kRjBQOQQPw",
            "AccountId": "BYsCoPKAWIEpRN",
            "Name": "Kent-Lyons",
            "FirstName": "Alexander",
            "LastName": "Chambers",
            "ContactEMail": "Alexander.Chambers@KentLyons.com",
            "MailingCity": "New Nicholaschester",
        },
    ]

    table_name = "raw.test_query"

    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    result = databricks.to_df(f"select * from {table_name}")
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(df)


def test_create_table():
    source_data = [
        {
            "Id": "wRACnHTeuw",
            "AccountId": "rWFuwDHxfGh",
            "Name": "Scott-Merritt",
            "FirstName": "Melody",
            "LastName": "Cook",
            "ContactEMail": "Melody.Cook@ScottMerritt.com",
            "MailingCity": "Elizabethfurt",
        },
        {
            "Id": "CFfTlqagNlpDu",
            "AccountId": "TtGjFbCTWJgdPT",
            "Name": "Mann-Warren",
            "FirstName": "Wayne",
            "LastName": "Morrison",
            "ContactEMail": "Wayne.Morrison@MannWarren.com",
            "MailingCity": "Kathrynmouth",
        },
        {
            "Id": "SNaTEOiGfPJ",
            "AccountId": "KxPqtfmCHA",
            "Name": "Peters, Perez and Bowman",
            "FirstName": "James",
            "LastName": "Ortiz",
            "ContactEMail": "James.Ortiz@PetersPerezandBowman.com",
            "MailingCity": "Michaelland",
        },
        {
            "Id": "yzdeNFYNPJWF",
            "AccountId": "wsmjsVQpKWUbA",
            "Name": "Estrada-Hayes",
            "FirstName": "Vanessa",
            "LastName": "Guzman",
            "ContactEMail": "Vanessa.Guzman@EstradaHayes.com",
            "MailingCity": "Barnesport",
        },
        {
            "Id": "kRjBQOQQPw",
            "AccountId": "BYsCoPKAWIEpRN",
            "Name": "Kent-Lyons",
            "FirstName": "Alexander",
            "LastName": "Chambers",
            "ContactEMail": "Alexander.Chambers@KentLyons.com",
            "MailingCity": "New Nicholaschester",
        },
    ]
    table_name = "raw.test_table"

    # Check if the table exists before creating it. If it does, delete it
    if databricks._check_if_table_exists("raw.test_table"):
        databricks.drop_table(table_name)
    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    databricks.create_table_from_pandas(table_name, df)

    result = databricks.to_df(f"SELECT * FROM {table_name}")
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(df)


def test_append():
    source_data = [
        {
            "Id": "wRACnHTeuw",
            "AccountId": "rWFuwDHxfGh",
            "Name": "Scott-Merritt",
            "FirstName": "Melody",
            "LastName": "Cook",
            "ContactEMail": "Melody.Cook@ScottMerritt.com",
            "MailingCity": "Elizabethfurt",
        },
        {
            "Id": "CFfTlqagNlpDu",
            "AccountId": "TtGjFbCTWJgdPT",
            "Name": "Mann-Warren",
            "FirstName": "Wayne",
            "LastName": "Morrison",
            "ContactEMail": "Wayne.Morrison@MannWarren.com",
            "MailingCity": "Kathrynmouth",
        },
        {
            "Id": "SNaTEOiGfPJ",
            "AccountId": "KxPqtfmCHA",
            "Name": "Peters, Perez and Bowman",
            "FirstName": "James",
            "LastName": "Ortiz",
            "ContactEMail": "James.Ortiz@PetersPerezandBowman.com",
            "MailingCity": "Michaelland",
        },
        {
            "Id": "yzdeNFYNPJWF",
            "AccountId": "wsmjsVQpKWUbA",
            "Name": "Estrada-Hayes",
            "FirstName": "Vanessa",
            "LastName": "Guzman",
            "ContactEMail": "Vanessa.Guzman@EstradaHayes.com",
            "MailingCity": "Barnesport",
        },
        {
            "Id": "kRjBQOQQPw",
            "AccountId": "BYsCoPKAWIEpRN",
            "Name": "Kent-Lyons",
            "FirstName": "Alexander",
            "LastName": "Chambers",
            "ContactEMail": "Alexander.Chambers@KentLyons.com",
            "MailingCity": "New Nicholaschester",
        },
        {
            "Id": "Append Test",
            "AccountId": "Updated!EHNYKjSZsiy",
            "Name": "new Append-2",
            "FirstName": "Appended",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Append!Jamesport",
        },
    ]

    data = [
        {
            "Id": "Append Test",
            "AccountId": "Updated!EHNYKjSZsiy",
            "Name": "new Append-2",
            "FirstName": "Appended",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Append!Jamesport",
        }
    ]

    table_name = "raw.test_table"

    source_df = pd.DataFrame(source_data)
    source_df = source_df.sort_values(by="Id")
    source_df = source_df.reset_index(drop=True)

    df = pd.DataFrame(data)

    databricks.insert_into(table_name=table_name, df=df, if_exists="append")

    result = databricks.to_df(f"SELECT * FROM {table_name}")
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(source_df)


def test_full_refresh():
    # Assert type and values returned after full refresh
    source_data = [
        {
            "Id": "wRACnHTeuw",
            "AccountId": "rWFuwDHxfGh",
            "Name": "Scott-Merritt",
            "FirstName": "Melody",
            "LastName": "Cook",
            "ContactEMail": "Melody.Cook@ScottMerritt.com",
            "MailingCity": "Elizabethfurt",
        },
        {
            "Id": "CFfTlqagNlpDu",
            "AccountId": "TtGjFbCTWJgdPT",
            "Name": "Mann-Warren",
            "FirstName": "Wayne",
            "LastName": "Morrison",
            "ContactEMail": "Wayne.Morrison@MannWarren.com",
            "MailingCity": "Kathrynmouth",
        },
        {
            "Id": "SNaTEOiGfPJ",
            "AccountId": "KxPqtfmCHA",
            "Name": "Peters, Perez and Bowman",
            "FirstName": "James",
            "LastName": "Ortiz",
            "ContactEMail": "James.Ortiz@PetersPerezandBowman.com",
            "MailingCity": "Michaelland",
        },
        {
            "Id": "yzdeNFYNPJWF",
            "AccountId": "wsmjsVQpKWUbA",
            "Name": "Estrada-Hayes",
            "FirstName": "Vanessa",
            "LastName": "Guzman",
            "ContactEMail": "Vanessa.Guzman@EstradaHayes.com",
            "MailingCity": "Barnesport",
        },
        {
            "Id": "kRjBQOQQPw",
            "AccountId": "BYsCoPKAWIEpRN",
            "Name": "Kent-Lyons",
            "FirstName": "Alexander",
            "LastName": "Chambers",
            "ContactEMail": "Alexander.Chambers@KentLyons.com",
            "MailingCity": "New Nicholaschester",
        },
    ]
    table_name = "raw.test_table"

    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    databricks.insert_into(table_name=table_name, df=df, if_exists="replace")

    result = databricks.to_df(f"SELECT * FROM {table_name}")
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(df)


def test_upsert():
    # Upsert and check if the data type and values are correct
    source_data = [
        {
            "Id": "wRACnHTeuw",
            "AccountId": "rWFuwDHxfGh",
            "Name": "Scott-Merritt",
            "FirstName": "Melody",
            "LastName": "Cook",
            "ContactEMail": "Melody.Cook@ScottMerritt.com",
            "MailingCity": "Elizabethfurt",
        },
        {
            "Id": "CFfTlqagNlpDu",
            "AccountId": "TtGjFbCTWJgdPT",
            "Name": "Mann-Warren",
            "FirstName": "Wayne",
            "LastName": "Morrison",
            "ContactEMail": "Wayne.Morrison@MannWarren.com",
            "MailingCity": "Kathrynmouth",
        },
        {
            "Id": "SNaTEOiGfPJ",
            "AccountId": "KxPqtfmCHA",
            "Name": "Peters, Perez and Bowman",
            "FirstName": "James",
            "LastName": "Ortiz",
            "ContactEMail": "James.Ortiz@PetersPerezandBowman.com",
            "MailingCity": "Michaelland",
        },
        {
            "Id": "yzdeNFYNPJWF",
            "AccountId": "wsmjsVQpKWUbA",
            "Name": "Estrada-Hayes",
            "FirstName": "Vanessa",
            "LastName": "Guzman",
            "ContactEMail": "Vanessa.Guzman@EstradaHayes.com",
            "MailingCity": "Barnesport",
        },
        {
            "Id": "kRjBQOQQPw",
            "AccountId": "BYsCoPKAWIEpRN",
            "Name": "Kent-Lyons",
            "FirstName": "Alexander",
            "LastName": "Chambers",
            "ContactEMail": "Alexander.Chambers@KentLyons.com",
            "MailingCity": "New Nicholaschester",
        },
        {
            "Id": "UpsertTest2",
            "AccountId": "Updated!EHNYKjSZsiy",
            "Name": "new upsert-2",
            "FirstName": "Updated",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Updated!Jamesport",
        },
    ]

    data = [
        {
            "Id": "UpsertTest2",
            "AccountId": "Updated!EHNYKjSZsiy",
            "Name": "new upsert-2",
            "FirstName": "Updated",
            "LastName": "Carter2",
            "ContactEMail": "Adam.Carter@TurnerBlack.com",
            "MailingCity": "Updated!Jamesport",
        }
    ]

    table_name = "raw.test_table"
    pk = "Id"

    data = pd.DataFrame(data)

    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    databricks.insert_into(
        table_name=table_name, df=data, primary_key=pk, if_exists="update"
    )

    result = databricks.to_df(f"SELECT * FROM {table_name}")
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(df)


def test_discover_schema():
    table_name = "raw.c4c_test4"
    result = databricks.discover_schema(table_name)
    expected_result = {
        "Id": "string",
        "AccountId": "string",
        "Name": "string",
        "FirstName": "string",
        "LastName": "string",
        "ContactEMail": "string",
        "MailingCity": "string",
    }

    assert result == expected_result
