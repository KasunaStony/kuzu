#pragma once

#include "hash_index_header.h"
#include "hash_index_slot.h"

#include "src/storage/index/include/hash_index_utils.h"
#include "src/storage/storage_structure/include/disk_array.h"
#include "src/storage/storage_structure/include/in_mem_file.h"

namespace graphflow {
namespace storage {

static constexpr page_idx_t INDEX_HEADER_ARRAY_HEADER_PAGE_IDX = 0;
static constexpr page_idx_t P_SLOTS_HEADER_PAGE_IDX = 1;
static constexpr page_idx_t O_SLOTS_HEADER_PAGE_IDX = 2;

/**
 * Basic index file consists of three disk arrays: indexHeader, primary slots (pSlots), and overflow
 * slots (oSlots).
 *
 * 1. HashIndexHeader contains the current state of the hash tables (level and split information:
 * currentLevel, levelHashMask, higherLevelHashMask, nextSplitSlotId;  key data type).
 *
 * 2. Given a key, it is mapped to one of the pSlots based on its hash value and the level and
 * splitting info. The actual key and value are either stored in the pSlot, or in a chained overflow
 * slots (oSlots) of the pSlot. Each pSlot corresponds to an mutex, whose unique lock is obtained
 * before any insertions or deletions to that pSlot or its chained oSlots.
 *
 * The slot data structure:
 * Each slot (p/oSlot) consists of a slot header and several entries. The max number of entries in
 * slot is given by HashIndexConfig::SLOT_CAPACITY. The size of the slot is given by
 * (sizeof(SlotHeader) + (SLOT_CAPACITY * sizeof(Entry)).
 *
 * SlotHeader: [numEntries, validityMask, nextOvfSlotId]
 * Entry: [key (fixed sized part), node_offset]
 *
 * 3. oSlots are used to store entries that comes to the designated primary slot that has already
 * been filled to the capacity. Several overflow slots can be chained after the single primary slot
 * as a singly linked link-list. Each slot's SlotHeader has information about the next overflow slot
 * in the chain and also the number of filled entries in that slot.
 *
 *  */

struct SlotInfo {
    slot_id_t slotId;
    bool isPSlot;
};

class BaseHashIndex {
public:
    explicit BaseHashIndex(const DataType& keyDataType) : numEntries{0} {
        keyHashFunc = HashIndexUtils::initializeHashFunc(keyDataType.typeID);
    }

    virtual ~BaseHashIndex() = default;

protected:
    slot_id_t getPrimarySlotIdForKey(const uint8_t* key);
    inline void lockSlot(SlotInfo& slotInfo) {
        assert(slotInfo.isPSlot);
        shared_lock sLck{pSlotSharedMutex};
        pSlotsMutexes[slotInfo.slotId]->lock();
    }
    inline void unlockSlot(const SlotInfo& slotInfo) {
        assert(slotInfo.isPSlot);
        shared_lock sLck{pSlotSharedMutex};
        pSlotsMutexes[slotInfo.slotId]->unlock();
    }

protected:
    unique_ptr<HashIndexHeader> indexHeader;
    shared_mutex pSlotSharedMutex;
    hash_function_t keyHashFunc;
    vector<unique_ptr<mutex>> pSlotsMutexes;
    atomic<uint64_t> numEntries;
};

class HashIndexBuilder : public BaseHashIndex {

public:
    HashIndexBuilder(const string& fName, const DataType& keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries);

    // Note: append assumes that bulkRserve has been called before it and the index has reserved
    // enough space already.
    inline bool append(int64_t key, node_offset_t value) {
        return appendInternal(reinterpret_cast<const uint8_t*>(&key), value);
    }
    // TODO(Guodong): Add the support of string keys back to fix this.
    inline bool append(const char* key, node_offset_t value) {
        return appendInternal(reinterpret_cast<const uint8_t*>(key), value);
    }
    inline bool lookup(int64_t key, node_offset_t& result) {
        return lookupInternalWithoutLock(reinterpret_cast<const uint8_t*>(&key), result);
    }

    // Non-thread safe. This should only be called in the copyCSV and never be called in parallel.
    void flush();

private:
    bool appendInternal(const uint8_t* key, node_offset_t value);
    bool lookupInternalWithoutLock(const uint8_t* key, node_offset_t& result);

    uint32_t allocateSlots(bool isPSlot, uint32_t numSlots);
    template<bool IS_LOOKUP>
    bool lookupOrExistsInSlotWithoutLock(
        Slot* slot, const uint8_t* key, node_offset_t* result = nullptr);
    void insertToSlotWithoutLock(Slot* slot, const uint8_t* key, node_offset_t value);
    Slot* getSlot(const SlotInfo& slotInfo);

    inline uint32_t allocatePSlots(uint32_t numSlots) {
        return allocateSlots(true /* isPSlot */, numSlots);
    }
    inline uint32_t allocateOSlots(uint32_t numSlots) {
        return allocateSlots(false /* isPSlot */, numSlots);
    }

private:
    unique_ptr<FileHandle> fileHandle;
    unique_ptr<InMemDiskArrayBuilder<HashIndexHeader>> headerArray;
    shared_mutex oSlotsSharedMutex;
    unique_ptr<InMemDiskArrayBuilder<Slot>> pSlots;
    unique_ptr<InMemDiskArrayBuilder<Slot>> oSlots;
    in_mem_insert_function_t keyInsertFunc;
    in_mem_equals_function_t keyEqualsFunc;
    unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

} // namespace storage
} // namespace graphflow
