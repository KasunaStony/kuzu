#pragma once

#include "src/common/include/types.h"
#include "src/processor/include/memory_manager.h"
#include "src/processor/include/operator/overflow_blocks.h"
#include "src/processor/include/operator/physical/tuple/data_chunks.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

struct PayloadInfo {
    PayloadInfo(uint32_t elementSize, bool isStoredAsOverflow)
        : elementSize(elementSize), isStoredAsOverflow(isStoredAsOverflow) {}

    uint32_t elementSize;
    bool isStoredAsOverflow;
};

struct BlockAppendInfo {
    BlockAppendInfo(uint8_t* buffer, uint64_t numEntries)
        : buffer(buffer), numEntries(numEntries) {}

    uint8_t* buffer;
    uint64_t numEntries;
};

constexpr const uint64_t HT_BLOCK_SIZE = 262144; // By default, block size is 256KB

struct HTProbeStateForTestingOnly {
    unique_ptr<uint8_t*[]> pointers;
    uint64_t numEntries;
};

//! The HashTable data structure for HashJoin. for now, we assume hash only happens on nodeId.
//! Layout of tuple in blocks: |key|payload|prev|.
//! Inside payload, variable-sized values are stored in overflowBlocks.
class HashTable {
public:
    HashTable(MemoryManager& memoryManager, vector<PayloadInfo>& payloadInfos);

    void addDataChunks(
        DataChunk& keyDataChunk, uint64_t keyVectorIdx, DataChunks& payloadDataChunks);
    void buildHTDirectory();
    unique_ptr<HTProbeStateForTestingOnly> probeForTestingOnly(
        DataChunk& keyChunk, NodeIDVector& keyVector);

private:
    MemoryManager& memoryManager;

    uint64_t numEntries;
    uint64_t hashBitMask;
    uint64_t numBytesForFixedTuplePart;
    uint64_t htBlockCapacity;

    // The main memory blocks holding |key|payload|prev| fields
    vector<unique_ptr<BlockHandle>> htBlocks;
    unique_ptr<BlockHandle> htDirectory;
    ValueVector hashVec;
    // The overflow memory blocks for variable-sized values in tuples
    OverflowBlocks overflowBlocks;

    void allocateHTBlocks(uint64_t remaining, vector<BlockAppendInfo>& tuplesAppendInfos);

    inline void appendPayloadVectorAsFixSizedValues(ValueVector& vector, uint8_t* appendBuffer,
        uint64_t valueOffsetInVector, uint64_t appendCount, bool isSingleValue) const;
    inline void appendPayloadVectorAsOverflowValue(
        ValueVector& vector, uint8_t* appendBuffer, uint64_t appendCount);

    void appendKeyVector(NodeIDVector& vector, uint8_t* appendBuffer, uint64_t valueOffsetInVector,
        uint64_t appendCount) const;

    HashTable(const HashTable&) = delete; // We don't allow copy of ht
};

} // namespace processor
} // namespace graphflow