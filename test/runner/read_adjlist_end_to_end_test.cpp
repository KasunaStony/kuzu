#include <filesystem>

#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class EndToEndReadLists2BytesPerEdgeTest : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/2-bytes-per-edge/"; }
};

class EndToEndReadLists4BytesPerEdgeTest : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/4-bytes-per-edge/"; }
};

TEST_F(EndToEndReadLists2BytesPerEdgeTest, 2BytesPerEdgeTest) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs =
        TestHelper::parseTestFile("test/runner/queries/adj-list-reading/2-bytes-per-edge.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(EndToEndReadLists4BytesPerEdgeTest, 4BytesPerEdgeTest) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs =
        TestHelper::parseTestFile("test/runner/queries/adj-list-reading/4-bytes-per-edge.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}