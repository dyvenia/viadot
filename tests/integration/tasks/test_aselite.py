import pandas as pd

from viadot.tasks import ASELiteToDF


def test_aselite_to_df():
    query = """SELECT TOP (10) [usageid]
      ,[configid]
      ,[verticalid]
      ,[textgroupid]
      ,[nr]
      ,[storedate]
  FROM [UCRMDEV_DESIGNER].[dbo].[PORTAL_APPLICATION_TEXTUSAGE]"""
    task = ASELiteToDF()
    df = task.run(query=query)
    assert isinstance(df, pd.DataFrame)
