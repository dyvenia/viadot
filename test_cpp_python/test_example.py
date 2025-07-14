import example

sap = example.MockSapRfcConnector()
connected = sap.connect(
    user="demo",
    passwd="demo",
    ashost="sap.example.com",
    sysnr="00",
    client="100"
)
print(f"Connected: {connected}")

print(f"Connection handle: {sap.con()}")
print(f"Check connection: {sap.check_connection()}")

print("Function description for RFC_READ_TABLE:")
for param in sap.get_function_description("RFC_READ_TABLE"):
    print(param)

print("Call RFC_PING:")
print(sap.call("RFC_PING", {}))

print("Call RFC_READ_TABLE with params:")
print(sap.call("RFC_READ_TABLE", {"QUERY_TABLE": "SFLIGHT"}))

metadata = sap.get_table_metadata("SFLIGHT")
print("SFLIGHT table metadata:")
for field in metadata:
    print(field)

print("Closing connection...")
sap.close_connection()
print(f"Check connection after close: {sap.check_connection()}")
print(f"Connection handle after close: {sap.con()}")
print("Function description after close:")
print(sap.get_function_description("RFC_READ_TABLE"))
print("Call after close:")
print(sap.call("RFC_PING", {})) 