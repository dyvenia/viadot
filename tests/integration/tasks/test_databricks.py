from unittest import result
from pandas import DataFrame
import pyspark
from viadot.sources import Databricks

databricks = Databricks()

def test_df_to_sparkDf():
    list = [{"some":"data"}]
    sparkDf = databricks.to_spark_df(list)
    assert type(sparkDf) is pyspark.sql.dataframe.DataFrame

def test_sparkDf_to_df():
    list = [{"some":"data"}]
    sparkDf = databricks.to_spark_df(list)
    df = databricks.spark_to_pandas(sparkDf)
    assert type(df) is DataFrame

def test_query():
    # Look up test_query table and check if the type and results returned are correct
    data_raw =[{"Id": "wRACnHTeuw", "AccountId": "rWFuwDHxfGh", "Name": "Scott-Merritt", "FirstName": "Melody", "LastName": "Cook", "ContactEMail": "Melody.Cook@ScottMerritt.com", "MailingCity": "Elizabethfurt"}, {"Id": "CFfTlqagNlpDu", "AccountId": "TtGjFbCTWJgdPT", "Name": "Mann-Warren", "FirstName": "Wayne", "LastName": "Morrison", "ContactEMail": "Wayne.Morrison@MannWarren.com", "MailingCity": "Kathrynmouth"}, {"Id": "SNaTEOiGfPJ", "AccountId": "KxPqtfmCHA", "Name": "Peters, Perez and Bowman", "FirstName": "James", "LastName": "Ortiz", "ContactEMail": "James.Ortiz@PetersPerezandBowman.com", "MailingCity": "Michaelland"}, {"Id": "yzdeNFYNPJWF", "AccountId": "wsmjsVQpKWUbA", "Name": "Estrada-Hayes", "FirstName": "Vanessa", "LastName": "Guzman", "ContactEMail": "Vanessa.Guzman@EstradaHayes.com", "MailingCity": "Barnesport"}, {"Id": "kRjBQOQQPw", "AccountId": "BYsCoPKAWIEpRN", "Name": "Kent-Lyons", "FirstName": "Alexander", "LastName": "Chambers", "ContactEMail": "Alexander.Chambers@KentLyons.com", "MailingCity": "New Nicholaschester"}]
    df = databricks.to_df(data_raw)
    result = databricks.query("select * from raw.test_query")
    df = df.sort_values(by='Id')
    result = result.sort_values(by='Id')
    df = df.reset_index(drop=True)
    result = result.reset_index(drop=True)
    assert result.equals(df)

def test_read_table():
    # Read table and assert its type and values
    data_raw =[{"Id": "wRACnHTeuw", "AccountId": "rWFuwDHxfGh", "Name": "Scott-Merritt", "FirstName": "Melody", "LastName": "Cook", "ContactEMail": "Melody.Cook@ScottMerritt.com", "MailingCity": "Elizabethfurt"}, {"Id": "CFfTlqagNlpDu", "AccountId": "TtGjFbCTWJgdPT", "Name": "Mann-Warren", "FirstName": "Wayne", "LastName": "Morrison", "ContactEMail": "Wayne.Morrison@MannWarren.com", "MailingCity": "Kathrynmouth"}, {"Id": "SNaTEOiGfPJ", "AccountId": "KxPqtfmCHA", "Name": "Peters, Perez and Bowman", "FirstName": "James", "LastName": "Ortiz", "ContactEMail": "James.Ortiz@PetersPerezandBowman.com", "MailingCity": "Michaelland"}, {"Id": "yzdeNFYNPJWF", "AccountId": "wsmjsVQpKWUbA", "Name": "Estrada-Hayes", "FirstName": "Vanessa", "LastName": "Guzman", "ContactEMail": "Vanessa.Guzman@EstradaHayes.com", "MailingCity": "Barnesport"}, {"Id": "kRjBQOQQPw", "AccountId": "BYsCoPKAWIEpRN", "Name": "Kent-Lyons", "FirstName": "Alexander", "LastName": "Chambers", "ContactEMail": "Alexander.Chambers@KentLyons.com", "MailingCity": "New Nicholaschester"}]
    df = databricks.to_df(data_raw)
    result = databricks.read_table("raw.test_query")
    df = df.sort_values(by='Id')
    result = result.sort_values(by='Id')
    df = df.reset_index(drop=True)
    result = result.reset_index(drop=True)
    assert result.equals(df)

def test_create_table():
    data_raw =[{"Id": "wRACnHTeuw", "AccountId": "rWFuwDHxfGh", "Name": "Scott-Merritt", "FirstName": "Melody", "LastName": "Cook", "ContactEMail": "Melody.Cook@ScottMerritt.com", "MailingCity": "Elizabethfurt"}, {"Id": "CFfTlqagNlpDu", "AccountId": "TtGjFbCTWJgdPT", "Name": "Mann-Warren", "FirstName": "Wayne", "LastName": "Morrison", "ContactEMail": "Wayne.Morrison@MannWarren.com", "MailingCity": "Kathrynmouth"}, {"Id": "SNaTEOiGfPJ", "AccountId": "KxPqtfmCHA", "Name": "Peters, Perez and Bowman", "FirstName": "James", "LastName": "Ortiz", "ContactEMail": "James.Ortiz@PetersPerezandBowman.com", "MailingCity": "Michaelland"}, {"Id": "yzdeNFYNPJWF", "AccountId": "wsmjsVQpKWUbA", "Name": "Estrada-Hayes", "FirstName": "Vanessa", "LastName": "Guzman", "ContactEMail": "Vanessa.Guzman@EstradaHayes.com", "MailingCity": "Barnesport"}, {"Id": "kRjBQOQQPw", "AccountId": "BYsCoPKAWIEpRN", "Name": "Kent-Lyons", "FirstName": "Alexander", "LastName": "Chambers", "ContactEMail": "Alexander.Chambers@KentLyons.com", "MailingCity": "New Nicholaschester"}]
    table_name = "raw.test_table"
    df = databricks.to_df(data_raw)
    if databricks.spark._jsparkSession.catalog().tableExists('raw', 'test_table'):
        databricks.delete_table(table_name)
    databricks.create_table(table_name, df)
    result = databricks.read_table(table_name)
    df = df.sort_values(by='Id')
    result = result.sort_values(by='Id')
    df = df.reset_index(drop=True)
    result = result.reset_index(drop=True)
    assert result.equals(df)

def test_full_refresh():
    # Assert type and values returned after full refresh
    data_raw =[{"Id": "wRACnHTeuw", "AccountId": "rWFuwDHxfGh", "Name": "Scott-Merritt", "FirstName": "Melody", "LastName": "Cook", "ContactEMail": "Melody.Cook@ScottMerritt.com", "MailingCity": "Elizabethfurt"}, {"Id": "CFfTlqagNlpDu", "AccountId": "TtGjFbCTWJgdPT", "Name": "Mann-Warren", "FirstName": "Wayne", "LastName": "Morrison", "ContactEMail": "Wayne.Morrison@MannWarren.com", "MailingCity": "Kathrynmouth"}, {"Id": "SNaTEOiGfPJ", "AccountId": "KxPqtfmCHA", "Name": "Peters, Perez and Bowman", "FirstName": "James", "LastName": "Ortiz", "ContactEMail": "James.Ortiz@PetersPerezandBowman.com", "MailingCity": "Michaelland"}, {"Id": "yzdeNFYNPJWF", "AccountId": "wsmjsVQpKWUbA", "Name": "Estrada-Hayes", "FirstName": "Vanessa", "LastName": "Guzman", "ContactEMail": "Vanessa.Guzman@EstradaHayes.com", "MailingCity": "Barnesport"}, {"Id": "kRjBQOQQPw", "AccountId": "BYsCoPKAWIEpRN", "Name": "Kent-Lyons", "FirstName": "Alexander", "LastName": "Chambers", "ContactEMail": "Alexander.Chambers@KentLyons.com", "MailingCity": "New Nicholaschester"}]
    table_name = "raw.test_table"
    df = databricks.to_df(data_raw)
    databricks.full_refresh(table_name, df)
    result = databricks.read_table(table_name)
    df = df.sort_values(by='Id')
    result = result.sort_values(by='Id')
    df = df.reset_index(drop=True)
    result = result.reset_index(drop=True)
    assert result.equals(df)

def test_upsert():
    # Upsert and check if the data type and values are correct
    data_raw =[{"Id": "wRACnHTeuw", "AccountId": "Upserted!", "Name": "Upsert-Merritt", "FirstName": "Melody", "LastName": "Cook", "ContactEMail": "Melody.Cook@ScottMerritt.com", "MailingCity": "Elizabethfurt"}, {"Id": "CFfTlqagNlpDu", "AccountId": "TtGjFbCTWJgdPT", "Name": "Mann-Warren", "FirstName": "Wayne", "LastName": "Morrison", "ContactEMail": "Wayne.Morrison@MannWarren.com", "MailingCity": "Kathrynmouth"}, {"Id": "SNaTEOiGfPJ", "AccountId": "KxPqtfmCHA", "Name": "Peters, Perez and Bowman", "FirstName": "James", "LastName": "Ortiz", "ContactEMail": "James.Ortiz@PetersPerezandBowman.com", "MailingCity": "Michaelland"}, {"Id": "yzdeNFYNPJWF", "AccountId": "wsmjsVQpKWUbA", "Name": "Estrada-Hayes", "FirstName": "Vanessa", "LastName": "Guzman", "ContactEMail": "Vanessa.Guzman@EstradaHayes.com", "MailingCity": "Barnesport"}, {"Id": "kRjBQOQQPw", "AccountId": "BYsCoPKAWIEpRN", "Name": "Kent-Lyons", "FirstName": "Alexander", "LastName": "Chambers", "ContactEMail": "Alexander.Chambers@KentLyons.com", "MailingCity": "New Nicholaschester"}]
    data = [{"Id": "wRACnHTeuw", "AccountId": "Upserted!", "Name": "Upsert-Merritt", "FirstName": "Melody", "LastName": "Cook", "ContactEMail": "Melody.Cook@ScottMerritt.com", "MailingCity": "Elizabethfurt"}]
    df = databricks.to_df(data_raw)
    table_name = "raw.test_table"
    match = {"Id": "main.Id",
            "AccountId": "updates.AccountId",
            "Name": "updates.Name",
            "FirstName": "updates.FirstName",
            "LastName": "updates.LastName",
            "ContactEMail": "updates.ContactEMail",
            "MailingCity": "main.MailingCity"}

    notMatch = {
            "Id": "updates.Id",
            "AccountId": "updates.AccountId",
            "Name": "updates.Name",
            "FirstName": "updates.FirstName",
            "LastName": "updates.LastName",
            "ContactEMail": "updates.ContactEMail",
            "MailingCity": "updates.MailingCity"
            }

    databricks.upsert(table_name, data, match, notMatch, "Id")
    result = databricks.read_table(table_name)
    df = df.sort_values(by='Id')
    result = result.sort_values(by='Id')
    df = df.reset_index(drop=True)
    result = result.reset_index(drop=True)
    assert result.equals(df)