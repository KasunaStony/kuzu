#include <filesystem>

#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "test/test_utility/include/db_loaded_test.h"
#include "test/test_utility/include/test_helper.h"
using ::testing::Test;
using namespace graphflow::testing;

class TinySnbProcessorTest : public DBLoadedTest {};

TEST_F(TinySnbProcessorTest, StructuralQueries) {
    unique_ptr<TestSuiteQueryConfig> queryConfig;
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/nodes.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/paths.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/stars.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, FilteredQueries) {
    unique_ptr<TestSuiteQueryConfig> queryConfig;
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/id_comparison.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/nodes.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/paths.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/stars.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/str_operations.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig =
        TestHelper::parseTestFile("test/runner/queries/filtered/unstructured_properties.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/load_csv.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, FrontierQueries) {
    unique_ptr<TestSuiteQueryConfig> queryConfig;
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/frontier.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, DateDataTypeTests) {
    unique_ptr<TestSuiteQueryConfig> queryConfig;
    queryConfig = TestHelper::parseTestFile("test/runner/queries/data_types/date_data_type.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
}
