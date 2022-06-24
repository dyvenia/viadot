from pandas import DataFrame
import pyspark
from viadot.sources import Databricks
import pandas as pd

databricks = Databricks()


def test_query():
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

    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    result = databricks.query("select * from raw.test_query")
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(df)


def test_to_df():
    # Read table and assert its type and values
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

    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    result = databricks.to_df("raw.test_query")
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
    if databricks.sc._jsparkSession.catalog().tableExists("raw", "test_table"):
        databricks.delete_table(table_name)
    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    databricks.create_table(table_name, df)

    result = databricks.to_df(table_name)
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(df)


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

    databricks.full_refresh(table_name, df)

    result = databricks.to_df(table_name)
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

    match = "Id= main.Id, AccountId= updates.AccountId, Name= updates.Name, FirstName= updates.FirstName, LastName= updates.LastName, ContactEMail= updates.ContactEMail, MailingCity= main.MailingCity"
    data = pd.DataFrame(data)
    query = databricks.build_merge_query(table_name, "Id", data, match)

    df = pd.DataFrame(source_data)
    df = df.sort_values(by="Id")
    df = df.reset_index(drop=True)

    databricks.query(query)

    result = databricks.to_df(table_name)
    result = result.sort_values(by="Id")
    result = result.reset_index(drop=True)

    assert result.equals(df)
