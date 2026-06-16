from unittest.mock import MagicMock, patch

from pydantic import ValidationError
import pytest

from viadot.sources.jira import Jira, JiraCredentials


@pytest.fixture
def credentials():
    """Reusable raw Jira credentials dict."""
    return {"CLIENT_ID": "test-id", "CLIENT_SECRET": "test-secret"}


@pytest.fixture
def field_api_response():
    """Trimmed response from the Jira GET /field endpoint."""
    return [
        {
            "id": "customfield_1",
            "key": "customfield_1",
            "name": "Description of Change",
            "untranslatedName": "Description of Change",
            "custom": True,
            "orderable": True,
            "navigable": True,
            "searchable": True,
            "clauseNames": [
                "Description of Change",
                "Description of Change[Paragraph]",
            ],
            "schema": {
                "type": "string",
                "custom": "com.atlassian.jira.plugin.system.customfieldtypes:textarea",
                "customId": 1,
            },
        },
        {
            "id": "customfield_2",
            "key": "customfield_2",
            "name": "Planned implementation date",
            "untranslatedName": "Planned implementation date",
            "custom": True,
            "orderable": True,
            "navigable": True,
            "searchable": True,
            "clauseNames": [
                "Planned implementation date[Date]",
                "Planned implementation date",
            ],
            "schema": {
                "type": "date",
                "custom": "com.atlassian.jira.plugin.system.customfieldtypes:datepicker",
                "customId": 2,
            },
        },
    ]


@pytest.fixture
def jira(credentials):
    """Reusable Jira instance with valid credentials."""
    return Jira(credentials=credentials)


def test_credentials_initialization(credentials):
    creds = JiraCredentials(**credentials)
    assert credentials["CLIENT_ID"] == creds.CLIENT_ID
    assert credentials["CLIENT_SECRET"] == creds.CLIENT_SECRET


def test_missing_field_raises():
    with pytest.raises(ValidationError):
        JiraCredentials(CLIENT_ID="only-id")


def test_dict_credentials_are_validated(credentials):
    jira = Jira(credentials=credentials)
    assert jira.credentials == credentials


def test_invalid_credentials_raise():
    with pytest.raises(ValidationError):
        Jira(credentials={"CLIENT_ID": "only-id"})  # type: ignore


def test_fetch_token(jira):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"access_token": "abc123"}
    with patch(
        "viadot.sources.jira.requests.post", return_value=mock_resp
    ) as mock_post:
        assert jira._get_access_token() == "abc123"

        mock_post.assert_called_once()
        payload = mock_post.call_args.kwargs["json"]
        assert payload["grant_type"] == "client_credentials"
        assert payload["client_id"] == "test-id"
        assert payload["client_secret"] == "test-secret"  # noqa: S105
        assert jira._access_token == "abc123"  # noqa: S105


def test_headers_contain_bearer_token(jira):
    jira._get_access_token = MagicMock(return_value="tok")
    headers = jira._build_headers()
    assert headers["Authorization"] == "Bearer tok"
    assert headers["Accept"] == "application/json"


def test_builds_url_and_returns_json(jira):
    jira._get_cloud_id = MagicMock(return_value="cloud-1")
    jira._build_headers = MagicMock(return_value={"Authorization": "Bearer test_token"})
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"ok": True}
    with patch(
        "viadot.sources.jira.handle_api_response", return_value=mock_resp
    ) as mock_h:
        result = jira._api_call("search/jql", params={"jql": "x"})

    assert result == {"ok": True}
    kwargs = mock_h.call_args.kwargs
    assert (
        kwargs["url"]
        == "https://api.atlassian.com/ex/jira/cloud-1/rest/api/3/search/jql"
    )
    assert kwargs["method"] == "GET"
    assert kwargs["params"] == {"jql": "x"}


def test_extracts_values_from_dict_response(jira):
    jira._api_call = MagicMock(return_value={"values": [{"id": "a"}]})
    assert jira._fetch_fields() == [{"id": "a"}]


def test_dict_response_without_values_returns_empty(jira):
    jira._api_call = MagicMock(return_value={"total": 0})
    assert jira._fetch_fields() == []


def test_standard_fields(jira):
    assert jira._resolve_field_path("Summary", {}) == "fields.summary"
    assert jira._resolve_field_path("Current Status", {}) == "fields.status.name"
    assert (
        jira._resolve_field_path("Reporter: Email", {})
        == "fields.reporter.emailAddress"
    )


@pytest.mark.parametrize(
    ("schema_type", "expected_suffix"),
    [
        ("option", ".value"),
        ("array", ".value"),
        ("user", ".displayName"),
        ("priority", ".name"),
        ("status", ".name"),
        ("issuetype", ".name"),
        ("project", ".name"),
        ("resolution", ".name"),
        ("string", ""),  # not in mapping
        ("number", ""),  # not in mapping
    ],
)
def test_custom_field_schema_suffix(jira, schema_type, expected_suffix):
    field_map = {"Custom": {"id": "customfield_1", "schema": {"type": schema_type}}}
    path = jira._resolve_field_path("Custom", field_map)
    assert path == f"fields.customfield_1{expected_suffix}"


def test_map_to_id_and_schema(jira):
    jira._fetch_fields = MagicMock(
        return_value=[
            {"name": "Summary", "id": "summary", "schema": {"type": "string"}},
            {
                "name": "Failure type",
                "id": "customfield_16187",
                "schema": {"type": "option"},
            },
        ]
    )
    field_map = jira._get_field_map()
    assert field_map["Summary"]["id"] == "summary"
    assert field_map["Failure type"]["id"] == "customfield_16187"
    assert field_map["Failure type"]["schema"] == {"type": "option"}


def test_builds_name_map(jira, field_api_response):
    jira._fetch_fields = MagicMock(return_value=field_api_response)
    field_map = jira._get_field_map()
    assert field_map.keys() == {
        "Description of Change",
        "Planned implementation date",
    }
    assert field_map["Description of Change"]["id"] == "customfield_1"
    assert field_map["Description of Change"]["schema"] == {
        "type": "string",
        "custom": "com.atlassian.jira.plugin.system.customfieldtypes:textarea",
        "customId": 1,
    }
    assert field_map["Planned implementation date"]["id"] == "customfield_2"
    assert field_map["Planned implementation date"]["schema"]["type"] == "date"


def test_jira_to_df_using_technical_fields(jira):
    issues = [{"key": "P-1", "fields": {"summary": "a", "customfield_1": "raw"}}]
    jira._fetch_issues = MagicMock(return_value=issues)

    df = jira.to_df(jql="q", technical_fields=["summary", "customfield_1"])
    assert "fields.summary" in df.columns
    assert df.loc[0, "fields.customfield_1"] == "raw"


def test_jira_to_df_using_standard_fields(jira):
    jira._get_field_map = MagicMock(return_value={})
    issues = [
        {"key": "P-1", "fields": {"summary": "Fix bug", "status": {"name": "Done"}}},
        {"key": "P-2", "fields": {"summary": "Feature", "status": {"name": "Open"}}},
    ]
    jira._fetch_issues = MagicMock(return_value=issues)

    df = jira.to_df(jql="q", fields=["Summary", "Current Status"])

    assert list(df.columns) == ["Key", "Summary", "Current Status"]
    assert df.loc[0, "Key"] == "P-1"
    assert df.loc[0, "Summary"] == "Fix bug"
    assert df.loc[1, "Current Status"] == "Open"

    called_fields = jira._fetch_issues.call_args.kwargs["fields"]
    assert set(called_fields) == {"summary", "status"}
