import pytest
import os
import logging

from viadot.flows import AzureSQLTransform
from viadot.tasks import AzureSQLDBQuery
from viadot.config import local_config

from azure.devops.v6_0.wiki import WikiClient
from msrest.authentication import BasicAuthentication

SCHEMA = "sandbox"
TABLE = "test1"
FQN = f"{SCHEMA}.{TABLE}"


@pytest.fixture()
def TEST_TABLE():
    run_sql_task = AzureSQLDBQuery()
    run_sql_task.run(f"CREATE TABLE {FQN} (id INT, name VARCHAR(25))")
    run_sql_task.run(f"INSERT INTO {FQN} VALUES (1, 'Mike')")
    yield
    run_sql_task.run(f"DROP TABLE {FQN}")


def test_azure_sql_transform(TEST_TABLE):
    flow = AzureSQLTransform(
        name="test",
        query=f"SELECT * FROM {FQN}",
    )
    result = flow.run()
    task = list(result.result.keys())[0]
    actual_result = result.result[task].result[0]
    assert list(actual_result) == [1, "Mike"]


def test_azure_sql_transform_save_query(caplog):
    file = "file_test.txt"
    project = local_config.get("AZURE-DEVOPS")["project"]
    wiki_identifier = local_config.get("AZURE-DEVOPS")["wiki_identifier"]
    devops_path = local_config.get("AZURE-DEVOPS")["devops_path_for_test"]
    personal_access_token = local_config.get("AZURE-DEVOPS")["personal_access_token"]
    organization_url = local_config.get("AZURE-DEVOPS")["organization_url"]
    credentials = BasicAuthentication("", personal_access_token)
    flow = AzureSQLTransform(
        name="test flow",
        query="""
        SELECT *
        FROM sys.tables t
        JOIN sys.schemas s
            ON t.schema_id = s.schema_id
    """,
        save_query=True,
        file_name=file,
        project=project,
        wiki_identifier=wiki_identifier,
        devops_path=devops_path,
        personal_access_token=personal_access_token,
        organization_url=organization_url,
    )
    with caplog.at_level(logging.INFO):
        flow.run()

    wiki = WikiClient(base_url=organization_url, creds=credentials)
    wiki.delete_page(project=project, wiki_identifier=wiki_identifier, path=devops_path)
    assert "Successfully loaded query" in caplog.text
    os.remove(file)
