"""Flow for pulling data from CloudForCustomers to Adls."""

from typing import Any

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    cloud_for_customers_to_df,
    df_to_adls,
)


@flow
def cloud_for_customers_to_adls(  # noqa: PLR0913
    # C4C
    cloud_for_customers_url: str | None = None,
    fields: list[str] | None = None,
    dtype: dict[str, Any] | None = None,
    endpoint: str | None = None,
    report_url: str | None = None,
    filter_params: dict[str, Any] | None = None,
    # ADLS
    adls_path: str | None = None,
    overwrite: bool = False,
    # Auth
    cloud_for_customers_credentials_secret: str | None = None,
    cloud_for_customers_config_key: str | None = None,
    adls_credentials_secret: str | None = None,
    adls_config_key: str | None = None,
    **kwargs: dict[str, Any] | None,
) -> None:
    """Download records from SAP Cloud for Customers and upload them to Azure Data Lake.

    Args:
        cloud_for_customers_url (str): The URL to the C4C API. For example,
            'https://myNNNNNN.crm.ondemand.com/c4c/v1/'.
        fields (list[str], optional): List of fields to put in DataFrame.
        dtype (dict, optional): The dtypes to use in the DataFrame.
        endpoint (str, optional): The API endpoint.
        report_url (str, optional): The API url in case of prepared report.
        filter_params (dict[str, Any], optional): Query parameters.
        adls_path (str): The destination path.
        overwrite (bool, optional): Whether to overwrite files in the lake. Defaults to
            False.
        cloud_for_customers_credentials_secret (str, optional): The name of the Azure
            Key Vault secret storing the C4C credentials. Defaults to None.
        cloud_for_customers_config_key (str, optional): The key in the viadot config
            holding relevant credentials. Defaults to None.
        adls_credentials_secret (str, optional): The name of the Azure Key Vault secret
            storing the ADLS credentials. Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
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

    return df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite,
    )
