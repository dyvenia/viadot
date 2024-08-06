"""Flow for pulling data from CloudForCustomers to Databricks."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    cloud_for_customers_to_df,
    df_to_databricks,
)


@flow
def cloud_for_customers_to_databricks(  # noqa: PLR0913, PLR0917
    # C4C
    cloud_for_customers_url: str,
    fields: list[str] | None = None,
    dtype: dict[str, Any] | None = None,
    endpoint: str | None = None,
    report_url: str | None = None,
    filter_params: dict[str, Any] | None = None,
    # Databricks
    databricks_table: str | None = None,
    databricks_schema: str | None = None,
    if_exists: Literal["replace", "skip", "fail"] = "fail",
    # Auth
    cloud_for_customers_credentials_secret: str | None = None,
    cloud_for_customers_config_key: str | None = None,
    databricks_credentials_secret: str | None = None,
    databricks_config_key: str | None = None,
    **kwargs: dict[str, Any] | None,
) -> None:
    """Download a file from SAP Cloud for Customers and upload it to Azure Data Lake.

    Args:
        cloud_for_customers_url (str): The URL to the C4C API. For example,
            'https://myNNNNNN.crm.ondemand.com/c4c/v1/'.
        fields (list[str], optional): List of fields to put in DataFrame.
        dtype (dict, optional): The dtypes to use in the DataFrame.
        endpoint (str, optional): The API endpoint.
        report_url (str, optional): The API url in case of prepared report.
        filter_params (dict[str, Any], optional): Query parameters.
        databricks_table (str): The name of the target table.
        databricks_schema (str, optional): The name of the target schema.
        if_exists (str, Optional): What to do if the table already exists. One of
            'replace', 'skip', and 'fail'.
        cloud_for_customers_credentials_secret (str, optional): The name of the Azure
            Key Vault secret storing the C4C credentials. Defaults to None.
        cloud_for_customers_config_key (str, optional): The key in the viadot config
            holding relevant credentials. Defaults to None.
        databricks_credentials_secret (str, optional): The name of the Azure Key Vault
            secret storing relevant credentials. Defaults to None.
        databricks_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        kwargs: The parameters to pass to the DataFrame constructor.
    """
    df = cloud_for_customers_to_df(
        url=cloud_for_customers_url,
        fields=fields,
        dtype=dtype,
        endpoint=endpoint,
        report_url=report_url,
        credentials_secret=cloud_for_customers_credentials_secret,
        config_key=cloud_for_customers_config_key,
        filter_params=filter_params,
        **kwargs,
    )

    return df_to_databricks(
        df=df,
        schema=databricks_schema,
        table=databricks_table,
        if_exists=if_exists,
        credentials_secret=databricks_credentials_secret,
        config_key=databricks_config_key,
    )
