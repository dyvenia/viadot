import os
from viadot.orchestration.prefect.flows import sql_server_to_parquet

PATH ="test_sql_server.parquet"
def test_sql_server_to_parquet():
    flow = sql_server_to_parquet(
    query=""" SELECT 
        t.name as table_name
                ,s.name as schema_name
                FROM sys.tables t
                JOIN sys.schemas s
                ON t.schema_id = s.schema_id""",
    path = PATH,
    sql_server_credentials_secret = "sql-server",
    )
    
    assert os.path.isfile(PATH) == True
    os.remove(PATH)