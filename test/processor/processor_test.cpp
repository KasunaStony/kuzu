#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"

using namespace graphflow::processor;

class ProcessorTest : public ::testing::Test {

protected:
    void SetUp() override {}

    void TearDown() override {}
};

class GraphStub : public Graph {

public:
    GraphStub() : Graph() {}
};

TEST(ProcessorTests, MultiThreadedScanTest) {
    unique_ptr<Graph> graph = make_unique<GraphStub>();
    auto morsel = make_shared<MorselsDesc>(1025013 /*numNodes*/);
    auto profiler = make_unique<Profiler>();
    auto memoryManager = make_unique<MemoryManager>();
    auto executionContext = ExecutionContext(*profiler, memoryManager.get());
    auto schema = Schema();
    auto group1Pos = schema.createGroup();
    schema.getGroup(group1Pos)->insertExpression("a._id");
    auto aIDPos = DataPos{0, 0};
    auto vectorsToCollect = vector<DataPos>{aIDPos};
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(vectorsToCollect,
        make_unique<ScanNodeID>(
            make_unique<ResultSetDescriptor>(schema), 0, aIDPos, morsel, executionContext, 0),
        RESULT_COLLECTOR, executionContext, 1));
    auto processor = make_unique<QueryProcessor>(10);
    auto result = processor->execute(plan.get(), 1);
    ASSERT_EQ(result->numTuples, 1025013);
}
