from typing import Any

from prefect import flow, task, unmapped
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_adls, sftp_to_df, sftp_list
import numpy as np
import pandas as pd
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

@flow(
    name="SFTP extraction to ADLS",
    description="Extract data from a SFTP server and "
    + "load it into Azure Data Lake Storage.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def sftp_to_adls(
    sftp_config_key: str | None = None,
    sftp_credentials_secret: str | None = None,
    file_name: str | None = None,
    sep: str = "\t",
    columns: list[str] | None = None,
    adls_config_key: str | None = None,
    adls_azure_key_vault_secret: str | None = None,
    adls_path: str | None = None,
    adls_path_overwrite: bool = True,
) -> None:
    r"""Flow to download data from a SFTP server to Azure Data Lake.

    Args:
        credentials (dict[str, Any], optional): SFTP credentials as a dictionary.
            Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        azure_key_vault_secret (str, optional): The name of the Azure Key Vault secret
            where credentials are stored. Defaults to None.
        file_name (str, optional): Path to the file in SFTP server. Defaults to None.
        sep (str, optional): The separator to use to read the CSV file.
            Defaults to "\t".
        columns (List[str], optional): Columns to read from the file. Defaults to None.
        adls_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        adls_azure_key_vault_secret (str, optional): The name of the Azure Key
            Vault secret containing a dictionary with ACCOUNT_NAME and Service Principal
            credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET) for the Azure Data Lake.
            Defaults to None.
        adls_path (str, optional): Azure Data Lake destination file path
            (with file name). Defaults to None.
        adls_path_overwrite (bool, optional): Whether to overwrite the file in ADLS.
            Defaults to True.
    """
    data_frame = sftp_to_df(
        config_key=sftp_config_key,
        azure_key_vault_secret=sftp_credentials_secret,
        file_name=file_name,
        sep=sep,
        columns=columns,
    )

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_azure_key_vault_secret,
        config_key=adls_config_key,
        overwrite=adls_path_overwrite,
    )


### Qwasi Submission ###
@task
def merge_df_list(df_list, use_case):

    df = pd.concat(df_list)
    df = df.replace(r"\n|\t", "", regex=True)

    ##
    if (use_case=="qwasi-submissions"):
        df["orderDate"] = (
            pd.to_datetime(df["orderDate"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype(str)
        )
        df["submissionDate"] = (
            pd.to_datetime(df["submissionDate"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype(str)
        )
        df["processedDate"] = (
            pd.to_datetime(df["processedDate"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype(str)
        )
        df["purchaseDate"] = (
            pd.to_datetime(df["purchaseDate"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype(str)
        )

        df["supplierID"] = pd.to_numeric(df["supplierID"], errors="coerce")
        df["supplierAccountASE"] = pd.to_numeric(df["supplierAccountASE"], errors="coerce")
        df["orderNumber"] = pd.to_numeric(df["orderNumber"], errors="coerce")
        df["purchaseDate"] = pd.to_numeric(df["purchaseDate"], errors="coerce")

        df["supplierID"] = df["supplierID"].fillna(0).astype(int)
        df["supplierAccountASE"] = df["supplierAccountASE"].fillna(0).astype(int)
        df["orderNumber"] = df["orderNumber"].fillna(0).astype(int)
        df["purchaseDate"] = df["purchaseDate"].fillna(0).astype(int)

        df["supplierID"] = pd.array(df["supplierID"], dtype=np.dtype("int32")).astype(str)
        df["supplierAccountASE"] = pd.array(
            df["supplierAccountASE"], dtype=np.dtype("int32")
        ).astype(str)
        df["orderNumber"] = pd.array(df["orderNumber"], dtype=np.dtype("int32")).astype(str)
        df["purchaseDate"] = pd.array(df["purchaseDate"], dtype=np.dtype("int32")).astype(
            str
        )

        df["supplierPostcode"] = df["supplierPostcode"].astype(str).str.replace("\.0", "")
        ##

        df = (
            df.astype(str)
            .replace("nan", np.nan, regex=True)
            .replace("NaT", np.nan, regex=True)
        )
        

        df = df[
            [
                "submissionID",
                "submissionDate",
                "customerID",
                "points",
                "submissionQuantity",
                "status",
                "submissionType",
                "accessMethod",
                "processedDate",
                "invoiceNumber",
                "supplierID",
                "supplierName",
                "supplierPostcode",
                "supplierAccountASE",
                "purchaseDate",
                "q1Core",
                "q2Core",
                "q2CoreOther",
                "q3Core",
                "q4Core",
                "q5Core",
                "q5CoreOther",
                "q6Core",
                "q6CoreOther",
                "q7Core",
                "q7CoreOther",
                "q8Core",
                "q8CoreOther",
                "q9Core",
                "q9CoreOther",
                "q10Core",
                "q10CoreOther",
                "q11Core",
                "q12Core",
                "q12CoreOther",
                "q13Core",
                "q14Core",
                "q15Core",
                "q16Core",
                "q16CoreOther",
                "q17Core",
                "q18Core",
                "orderNumber",
                "orderDate",
                "regionID",
                "wsaResponse",
                "externalSubmissionID",
                "redactedStatus",
                "growthStatus",
                "escalated",
                "rejectionCat",
                "source",
                "_viadot_downloaded_at_utc",
            ]
        ]
        df["wsaResponse"] = df.apply(
            lambda x: (
                "{Enable JavaScript and cookies to continue}"
                if len(str(x["wsaResponse"])) > 500
                and "Enable JavaScript and cookies to continue" in str(x["wsaResponse"])
                else x["wsaResponse"]
            ),
            axis=1,
        )
    elif (use_case=="qwasi-members"):
        
        df = df.replace(r"---", "0", regex=True)
        
        # Cleaning columns
        df["deactivatedDate"] = pd.to_datetime(df["deactivatedDate"], errors="coerce")
        df["deactivatedDate"].dt.strftime("%Y%m%d")

        # pd.to_numeric(s, errors='coerce')
        df["q1Core"] = pd.to_numeric(df["q1Core"], errors="coerce")
        df["q2Core"] = pd.to_numeric(df["q2Core"], errors="coerce")
        df["q3Core"] = pd.to_numeric(df["q3Core"], errors="coerce")
        df["q4Core"] = pd.to_numeric(df["q4Core"], errors="coerce")
        df["q5Core"] = pd.to_numeric(df["q5Core"], errors="coerce")
        df["q6Core"] = pd.to_numeric(df["q6Core"], errors="coerce")
        df["q7Core"] = pd.to_numeric(df["q7Core"], errors="coerce")
        df["q8Core"] = pd.to_numeric(df["q8Core"], errors="coerce")
        df["q9Core"] = pd.to_numeric(df["q9Core"], errors="coerce")

        df["q1Core"] = df["q1Core"].fillna(0).astype(int)
        df["q2Core"] = df["q2Core"].fillna(0).astype(int)
        df["q3Core"] = df["q3Core"].fillna(0).astype(int)
        df["q4Core"] = df["q4Core"].fillna(0).astype(int)
        df["q5Core"] = df["q5Core"].fillna(0).astype(int)
        df["q6Core"] = df["q6Core"].fillna(0).astype(int)
        df["q7Core"] = df["q7Core"].fillna(0).astype(int)
        df["q8Core"] = df["q8Core"].fillna(0).astype(int)
        df["q9Core"] = df["q9Core"].fillna(0).astype(int)

        df["q1Core"] = pd.array(df["q1Core"], dtype=np.dtype("int32")).astype(str)
        df["q2Core"] = pd.array(df["q2Core"], dtype=np.dtype("int32")).astype(str)
        df["q3Core"] = pd.array(df["q3Core"], dtype=np.dtype("int32")).astype(str)
        df["q4Core"] = pd.array(df["q4Core"], dtype=np.dtype("int32")).astype(str)
        df["q5Core"] = pd.array(df["q5Core"], dtype=np.dtype("int32")).astype(str)
        df["q6Core"] = pd.array(df["q6Core"], dtype=np.dtype("int32")).astype(str)
        df["q7Core"] = pd.array(df["q7Core"], dtype=np.dtype("int32")).astype(str)
        df["q8Core"] = pd.array(df["q8Core"], dtype=np.dtype("int32")).astype(str)
        df["q9Core"] = pd.array(df["q9Core"], dtype=np.dtype("int32")).astype(str)

        df = (
            df.astype(str)
            .replace("nan", np.nan, regex=False)
            .replace("NaT", np.nan, regex=False)
        )

        # Columns are specified to avoid errors if origin changes columns.
        df = df[
            [
                "CustomerID",
                "membershipDate",
                "status",
                "firstName",
                "lastName",
                "email",
                "msisdn",
                "companyName",
                "companyNumber",
                "regionID",
                "languageID",
                "address1",
                "address2",
                "town",
                "postcode",
                "currentPoints",
                "pendingPoints",
                "totalPoints",
                "pointsSpent",
                "modifiedDate",
                "modifiedBy",
                "accessMethod",
                "q1Core",
                "q2Core",
                "q3Core",
                "q4Core",
                "q5Core",
                "q6Core",
                "q7Core",
                "q7CoreOther",
                "q8Core",
                "q8CoreOther",
                "q9Core",
                "optinsms",
                "optinemail",
                "optinMarketing",
                "customerIP",
                "certInstaller",
                "ASEID",
                "segment",
                "lastLogin",
                "totalLogins",
                "activationDate",
                "anonymisedDate",
                "deactivatedDate",
                "expiredPoints",
                "expiringPoints",
                "expiringPeriodEarned",
                "expiringPointsByASE",
                "pointsModifiedDate",
                "numberOfSubmissions",
                "customerApprovedSubmissions",
                "customerWindows",
                "lastSubmissionDate",
                "tier",
                "vipLoginCount",
                "firstLogin",
                "title",
                "pendingInvoices",
                "sapUpdate",
                "hybridUpdate",
                "website",
                "facebook",
                "youtube",
                "instagram",
                "customerType",
                "primaryAccount",
                "usePrimaryOnly",
                "customerConfiguratorStatusID",
                "displayInstallerLead",
                "source",
                "salesRep",
                "_viadot_downloaded_at_utc",
            ]
        ]

    elif(use_case=="qwasi-submission-products"):
        df["submissionProductDate"] = pd.to_datetime(df["submissionProductDate"], errors="coerce").dt.strftime("%Y.%m.%d")
        df = (
            df.astype(str)
            .replace("nan", np.nan, regex=True)
            .replace("NaT", np.nan, regex=True)
        )
    
    elif(use_case=="qwasi-submission-rooms"):
        df["submissionID"] = df["submissionID"].astype("Int64").astype(str)
        df["regionID"] = df["regionID"].astype("Int64").astype(str)
        df = df.replace("nan", np.nan)

    else:
        df = (
            df.astype(str)
            .replace("nan", np.nan, regex=True)
            .replace("NaT", np.nan, regex=True)
        )
        
    return df



@flow(
    name="SFTP File List extraction to ADLS",
    description="SFTP File List extraction, compress to one parquet file and push into ADLS",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner
)
def sftp_stack_files_to_adls(
    sftp_credentials_secret: str | None = None,
    sep: str = "\t",
    columns: list[str] | None = None,
    use_case: str | None = None,
    sftp_matching_path: str | None = None,
    sftp_path: str | None = None,
    adls_path: str | None = None,
    adls_credentials_secret: str | None = None,
    adls_overwrite_path: bool = True,
):
    logger = logging.getLogger()  # Get the logger at the flow level
    logger.info(f"Flow started!")
    
    files_list= sftp_list(
        azure_key_vault_secret=sftp_credentials_secret,
        path=sftp_path,
        recursive=True,
        matching_path= sftp_matching_path,
    ) 
    # Apply sftp_to_df to each file name in the list
    df_list =  sftp_to_df.map(
        azure_key_vault_secret=unmapped(sftp_credentials_secret),
        file_name=files_list,
        sep=unmapped(sep),
    )
    
    if len(df_list)==0:
        # Log flow-level information before and after tasks are run
        logger.info("Df list is empty, review sftp_matching_path!")
        return None
    
    data_frame = merge_df_list(df_list, use_case)

    return df_to_adls(
        df=data_frame,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        overwrite= adls_overwrite_path,
    )


if __name__=="__main__":
    sftp_to_adls(
        file_name="_pbi_Respondent_level.csv",
        sftp_credentials_secret= "ipsos-sftp-access",
        adls_azure_key_vault_secret= "app-azure-cr-datalakegen2-dev",
        adls_path= "raw/marketing/ipsos/_pbi_Respondent_level.csv"
    )