import time
import sys
sys.path.insert(0, '/home/viadot/src')

from viadot.sources.sap_rfc import SAPRFC
from viadot.orchestration.prefect.utils import get_credentials
start_time = time.time()
credentials_sap = get_credentials(secret_name="sapsecret")
print(credentials_sap)

credentials_sap["ashost"] = "10.120.222.3"
sap_rfc = SAPRFC(credentials=credentials_sap,
 func="BBP_RFC_READ_TABLE",
 rfc_unique_id=["KNUMH", "KOPOS"])

print(sap_rfc.get_function_parameters("BBP_RFC_READ_TABLE"))

# start_query = time.time()

# sap_rfc.check_connection()

# sap_rfc.query(
#  sql="""
#  SELECT
#  KNUMH,
#  KOPOS,
#  KAPPL,
#  KSCHL,
#  KBETR
#  FROM
#  KONP
#  WHERE
#  KAPPL='KA'
#  """
# )



# df = sap_rfc.to_df()

# print(df)

# end_time = time.time()
# print(f"Time taken: {end_time - start_time} seconds")