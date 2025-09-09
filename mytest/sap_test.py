from math import e
import time
import sys
sys.path.insert(0, '/home/viadot/src')

# from viadot.sources.sap_rfc_old import SAPRFC
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

query_mara="""
            SELECT
                MATNR,
                MTART,
                ERSDA,
                CREATED_AT_TIME,
                LAEDA,
                ERNAM,
                AENAM,
                VPSTA,
                PSTAT,
                MBRSH,
                GEWEI,
                VOLUM,
                VOLEH,
                TEMPB,
                TRAGR,
                SPART,
                VABME,
                LAST_CHANGED_TIME,
                MATNR_EXTERNAL,
                ZZETYPE,
                MEINS,
                EAN11,
                EANNR,
                ZZEDMA,
                ZZEDMA2,
                MATKL,
                ZZMEINS,
                ZZEAN11,
                ZZEAN14,
                ZZGTINX
            FROM
                MARA
	    """

methods = ['to_df']

for method_name in methods:
    print(f"\nTesting {method_name}...")
    
    # Reset connection
    sap_rfc = SAPRFC(
        credentials=credentials_sap,
        func="BBP_RFC_READ_TABLE",
        # rfc_unique_id=["MATNR"],
        rfc_unique_id=None,
    )

    sap_rfc.query(sql=query_mara, sep="☩")
    
    start_time = time.time()
    if method_name == 'to_df_faster':
        df = sap_rfc.to_df_faster()
    elif method_name == 'to_df_polars':
        df = sap_rfc.to_df_polars()
    elif method_name == 'to_df_native':
        df = sap_rfc.to_df_native()
    elif method_name == 'to_df_original':
        df_original = sap_rfc.to_df_original()
        print(df_original)
    elif method_name == 'to_df':
        df = sap_rfc.to_csv_path()
        print(df)
exit()
#compare df and df_original if there are type differences
if df.dtypes.equals(df_original.dtypes):
    print("No type differences")
else:
    print("Type differences")
    print(df.dtypes)
    print(df_original.dtypes)

    end_time = time.time()
    print(f"{method_name}: {end_time - start_time:.3f} seconds")
    print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
    print(df)

#check if the df is the same as the df_original
if df.equals(df_original):
    print("The df is the same as the df_original")
else:
    #print differences
    # INSERT_YOUR_CODE
    # Print out the differences between df and df_original
    import pandas as pd

    # Find rows in df not in df_original
    diff1 = pd.concat([df, df_original, df_original]).drop_duplicates(keep=False)
    # Find rows in df_original not in df
    diff2 = pd.concat([df_original, df, df]).drop_duplicates(keep=False)

    print("Rows in df but not in df_original:")
    print(diff1)
    print("Rows in df_original but not in df:")
    print(diff2)

    # Optionally, show where values differ for matching indices
    try:
        comparison = df.compare(df_original, keep_shape=True, keep_equal=False)
        print("Cell-wise differences:")
        print(comparison)
    except Exception as e:
        print(f"Could not compare dataframes cell-wise: {e}")
    print("The df is not the same as the df_original")
exit()

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
     rfc_unique_id=None)
    start_query = time.time()
    sap_rfc.query(sql=query_mara, sep="☩")
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