#include <gtest/gtest.h>
#include "mock_sap_rfc_connector.h"
#include <string>
#include <map>
#include <vector>

// Helper credentials (dummy)
const std::string user = "demo";
const std::string passwd = "demo";
const std::string ashost = "sap.example.com";
const std::string sysnr = "00";
const std::string client = "100";

TEST(SapRfcConnectorTest, ConnectAndCon) {
    MockSapRfcConnector sap;
    bool connected = sap.connect(user, passwd, ashost, sysnr, client);
    EXPECT_TRUE(connected);
    EXPECT_NE(sap.con(), 0);
}

TEST(SapRfcConnectorTest, CheckConnection) {
    MockSapRfcConnector sap;
    sap.connect(user, passwd, ashost, sysnr, client);
    EXPECT_TRUE(sap.check_connection());
    sap.close_connection();
    EXPECT_FALSE(sap.check_connection());
}

TEST(SapRfcConnectorTest, CloseConnection) {
    MockSapRfcConnector sap;
    sap.connect(user, passwd, ashost, sysnr, client);
    sap.close_connection();
    EXPECT_FALSE(sap.check_connection());
    EXPECT_EQ(sap.con(), 0);
}

TEST(SapRfcConnectorTest, GetFunctionDescription) {
    MockSapRfcConnector sap;
    sap.connect(user, passwd, ashost, sysnr, client);
    auto desc = sap.get_function_description("RFC_READ_TABLE");
    EXPECT_TRUE(desc.size() > 0);
    sap.close_connection();
    auto desc2 = sap.get_function_description("RFC_READ_TABLE");
    EXPECT_EQ(desc2.size(), 0);
}

TEST(SapRfcConnectorTest, CallFunction) {
    MockSapRfcConnector sap;
    sap.connect(user, passwd, ashost, sysnr, client);
    std::map<std::string, std::string> params;
    auto result = sap.call("RFC_PING", params);
    EXPECT_EQ(result["status"], "pong");
    sap.close_connection();
    auto result2 = sap.call("RFC_PING", params);
    EXPECT_EQ(result2.size(), 0);
}

TEST(SapRfcConnectorTest, GetTableMetadata) {
    MockSapRfcConnector sap;
    sap.connect(user, passwd, ashost, sysnr, client);
    auto metadata = sap.get_table_metadata("SFLIGHT");
    EXPECT_TRUE(metadata.size() > 0);
    sap.close_connection();
    auto metadata2 = sap.get_table_metadata("SFLIGHT");
    EXPECT_EQ(metadata2.size(), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
} 