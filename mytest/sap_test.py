from math import e
import time
import sys
sys.path.insert(0, '/home/viadot/src')

from viadot.sources.sap_rfc import SAPRFC
from viadot.orchestration.prefect.utils import get_credentials
start_time = time.time()
credentials_sap = get_credentials(secret_name="sapsecret")
print(credentials_sap)

credentials_sap["ashost"] = "10.120.222.3"

ZSD0_RGNTS_PNL = False

query="""
        SELECT 
            VKORG,
            GJAHR,
            REGIO,
            VBELN,
            FKART,
            NETWR,
            KUNAG,
            FKDAT,
            BELNR,
            ZUONR,
            WAERK,
            LAND1,
            ERNAM,
            ERZET,
            ERDAT,
            MWSBK,
            VATDATE,
            VBTYP,
            FKDAT_RL,
            SFAKN,
            KALSM
        FROM VBRK
        WHERE VKORG IN ('167','185','186','187','193','401')
        """


if ZSD0_RGNTS_PNL:
    sap_rfc = SAPRFC(
        credentials=credentials_sap,
        func="/SAPDS/RFC_READ_TABLE",
        rfc_unique_id=["SPRAS", "REAGENT_PANEL_ID"],
    )
    sap_rfc.query(
        sql="""
        SELECT
            SPRAS,
            REAGENT_PANEL_ID,
            `TEXT`
        FROM
            ZSD0_RGNTS_PNL
    """,
        sep="☩",
    )
else:
    start_time = time.time()
    sap_rfc = SAPRFC(credentials=credentials_sap,
     func="BBP_RFC_READ_TABLE",
     rfc_unique_id=[])
    start_query = time.time()
    sap_rfc.query(sql=query, sep="☩")
    # sap_rfc.query(
    #  sql="""
    #  SELECT
    #  KNUMH,
    #  KOPOS,
    #  KAPPL,
    #  KSCHL,
    #  KBETR,
    #  KNUMA_BO,
    #  VERTN,
    #  KSTBM,
    #  KSTBW,
    #  PKWRT,
    #  MIKBAS,
    #  KLF_STG,
    #  BOMAT,
    #  FROM
    #  KONP
    #  """,
    #  sep="☩"
    # )
    end_time = time.time()
    # print(f"Time taken to query: {end_time - start_time} seconds")




# start_time = time.time()
df = sap_rfc.to_df()
# end_time = time.time()
# print(f"Time taken to to_df: {end_time - start_time} seconds")
print(df)