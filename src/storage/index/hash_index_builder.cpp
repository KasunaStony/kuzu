#include "storage/index/hash_index_builder.h"
#include <iostream>
using namespace kuzu::common;

namespace kuzu {
namespace storage {

slot_id_t BaseHashIndex::getPrimarySlotIdForKey(
    const HashIndexHeader& indexHeader_, const uint8_t* key, uint8_t* tag) {
    auto hash = keyHashFunc(key);
    if (tag != nullptr ) *tag = HashIndexUtils::compute_tag(hash);
    auto slotId = hash & indexHeader_.levelHashMask;
    if (slotId < indexHeader_.nextSplitSlotId) {
        slotId = hash & indexHeader_.higherLevelHashMask;
    }
    return slotId;
}

HashIndexBuilderInt64::HashIndexBuilderInt64(const std::string& fName, const LogicalType& keyDataType)
    : BaseHashIndex{keyDataType}, numEntries{0}, emptyOSlotID{0} {
    fileHandle =
        std::make_unique<FileHandle>(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    indexHeader = std::make_unique<HashIndexHeader>(keyDataType.getLogicalTypeID());
    fileHandle->addNewPage(); // INDEX_HEADER_ARRAY_HEADER_PAGE
    fileHandle->addNewPage(); // P_SLOTS_HEADER_PAGE
    fileHandle->addNewPage(); // O_SLOTS_HEADER_PAGE
    headerArray = std::make_unique<InMemDiskArrayBuilder<HashIndexHeader>>(
        *fileHandle, INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /* numElements */);
    pSlots = std::make_unique<InMemDiskArrayBuilder<Slot<int64_t>>>(
        *fileHandle, P_SLOTS_HEADER_PAGE_IDX, 0 /* numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    oSlots = std::make_unique<InMemDiskArrayBuilder<Slot<int64_t>>>(
        *fileHandle, O_SLOTS_HEADER_PAGE_IDX, 1 /* numElements */);
    allocatePSlots(2);
    if (keyDataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        inMemOverflowFile =
            std::make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(fName));
    }
    //keyInsertFunc = InMemHashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    //keyEqualsFunc = InMemHashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

void HashIndexBuilderInt64::bulkReserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries = getNumRequiredEntries(numEntries.load(), numEntries_);
    // Build from scratch.
    auto numRequiredSlots = (numRequiredEntries + HashIndexConstants::SLOT_CAPACITY - 1) /
                            HashIndexConstants::SLOT_CAPACITY;
    auto numSlotsOfCurrentLevel = 1 << indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < numRequiredSlots) {
        indexHeader->incrementLevel();
        numSlotsOfCurrentLevel = numSlotsOfCurrentLevel << 1;
    }
    if (numRequiredSlots > numSlotsOfCurrentLevel) {
        indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
    }
    allocatePSlots(numRequiredSlots);
    allocateOSlots(numRequiredSlots/20);
}

bool HashIndexBuilderInt64::appendInternal(const uint8_t* key, offset_t value) {
    uint8_t tag = 0;
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(*indexHeader, key, &tag), SlotType::PRIMARY};
    auto currentSlotInfo = pSlotInfo;

    Slot<int64_t>* primarySlot = getSlot(currentSlotInfo);
    Slot<int64_t>* currentSlot = nullptr;
    
    primarySlot->header.spinLock();

    if (lookupOrExistsInSlotWithoutLock<false>(primarySlot, key, tag)) {
        // Key already exists. No append is allowed.
            primarySlot->header.unlock();
            return false;
    }
    currentSlot = primarySlot;
    if (currentSlot->header.numEntries == HashIndexConstants::SLOT_CAPACITY) {
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;

        while (currentSlotInfo.slotId != 0) {

            oSlotsSharedMutex.lock_shared();
            currentSlot = getSlot(currentSlotInfo);
            oSlotsSharedMutex.unlock_shared();

            if (lookupOrExistsInSlotWithoutLock<false>(currentSlot, key, tag)) {
                // Key already exists. No append is allowed.
                primarySlot->header.unlock();
                return false;
            }
            if (currentSlot->header.numEntries < HashIndexConstants::SLOT_CAPACITY) {
                break;
            }
            currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        }
    }

    if (currentSlot->header.numEntries == HashIndexConstants::SLOT_CAPACITY) {
        auto ovfSlotId = getAOSlot();
        currentSlot->header.nextOvfSlotId = ovfSlotId;

        oSlotsSharedMutex.lock_shared();
        currentSlot = getSlot(SlotInfo{ovfSlotId, SlotType::OVF});
        oSlotsSharedMutex.unlock_shared();
    }

    insertToSlotWithoutLock(currentSlot, key, tag, value);
    primarySlot->header.unlock();
    numEntries.fetch_add(1);
    return true;
}

bool HashIndexBuilderInt64::lookupInternalWithoutLock(const uint8_t* key, offset_t& result) {
    uint8_t tag = 0;
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(*indexHeader, key, &tag), SlotType::PRIMARY};
    SlotInfo currentSlotInfo = pSlotInfo;
    Slot<int64_t>* currentSlot;
    while (currentSlotInfo.slotType == SlotType::PRIMARY || currentSlotInfo.slotId != 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<true /* lookup */>(currentSlot, key, tag, &result)) {
            return true;
        }
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;
    }
    return false;
}

uint32_t HashIndexBuilderInt64::allocatePSlots(uint32_t numSlotsToAllocate) {
    auto oldNumSlots = pSlots->getNumElements();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    pSlots->resize(newNumSlots, true /* setToZero */);
    for (uint64_t i = oldNumSlots; i < newNumSlots; i++) {
        pSlots->operator[](i) = Slot<int64_t>();
    }
    return oldNumSlots;
}

uint32_t HashIndexBuilderInt64::allocateOSlots(uint32_t numSlotsToAllocate) {
    auto oldNumSlots = oSlots->getNumElements();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    oSlots->resize(newNumSlots, true /* setToZero */);
    oSlotCapacity.store(newNumSlots);
    return oldNumSlots;
}

void HashIndexBuilderInt64::resizeOSlot() {
    if (emptyOSlotID.load() >= oSlotCapacity.load()) {
        oSlotsSharedMutex.lock();
        auto oldNumSlots = oSlots->getNumElements();
        auto addNewSlots = oldNumSlots / 2 + 1;
        oSlots->resize(oldNumSlots + addNewSlots, true /* setToZero */);
        oSlotCapacity.fetch_add(addNewSlots);
        oSlotsSharedMutex.unlock();
    }
}

Slot<int64_t>* HashIndexBuilderInt64::getSlot(const SlotInfo& slotInfo) {
    if (slotInfo.slotType == SlotType::PRIMARY) {
        return &pSlots->operator[](slotInfo.slotId);
    } else {
        return &oSlots->operator[](slotInfo.slotId);
    }
}

uint32_t HashIndexBuilderInt64::getAOSlot() {
    slot_id_t oSlotID = emptyOSlotID.fetch_add(1);
    if (oSlotID >= oSlotCapacity.load()) {
        resizeOSlot();
    }
    return oSlotID;
}

template<bool IS_LOOKUP>
bool HashIndexBuilderInt64::lookupOrExistsInSlotWithoutLock(
    Slot<int64_t>* slot, const uint8_t* key, const uint8_t tag, offset_t* result) {
    
    bool loopCon[HashIndexConstants::SLOT_CAPACITY];
    for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
        loopCon[entryPos] = slot->header.isEntryValid(entryPos) && 
                            tag == slot->header.getPartialHash(entryPos);
    }
    for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
        auto& entry = slot->entries[entryPos];
        if (loopCon[entryPos] && *(int64_t*)key == *(int64_t*)entry.data) {
            if constexpr (IS_LOOKUP) {
                memcpy(result, entry.data + indexHeader->numBytesPerKey, sizeof(offset_t));
            }
            return true;
        }
    }
    return false;
}

void HashIndexBuilderInt64::insertToSlotWithoutLock(
    Slot<int64_t>* slot, const uint8_t* key, const uint8_t tag, offset_t value) {
    // store the next empty slot pos in header directly
    assert(slot->header.numEntries < HashIndexConstants::SLOT_CAPACITY);
    uint8_t entryPos = slot->header.numEntries;
    memcpy(slot->entries[entryPos].data, key, NUM_BYTES_FOR_INT64_KEY);
    memcpy(slot->entries[entryPos].data + NUM_BYTES_FOR_INT64_KEY, &value, sizeof(common::offset_t));
    slot->header.setPartialHash(entryPos, tag);
    slot->header.setEntryValid(entryPos);
    slot->header.numEntries++;
}

void HashIndexBuilderInt64::flush() {
    indexHeader->numEntries = numEntries.load();
    headerArray->resize(1, true /* setToZero */);
    headerArray->operator[](0) = *indexHeader;
    headerArray->saveToDisk();
    pSlots->saveToDisk();
    oSlots->saveToDisk();
    if (indexHeader->keyDataTypeID == LogicalTypeID::STRING) {
        inMemOverflowFile->flush();
    }
}


HashIndexBuilderString::HashIndexBuilderString(const std::string& fName, const LogicalType& keyDataType)
    : BaseHashIndex{keyDataType}, numEntries{0} {
    fileHandle =
        std::make_unique<FileHandle>(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    indexHeader = std::make_unique<HashIndexHeader>(keyDataType.getLogicalTypeID());
    fileHandle->addNewPage(); // INDEX_HEADER_ARRAY_HEADER_PAGE
    fileHandle->addNewPage(); // P_SLOTS_HEADER_PAGE
    fileHandle->addNewPage(); // O_SLOTS_HEADER_PAGE
    headerArray = std::make_unique<InMemDiskArrayBuilder<HashIndexHeader>>(
        *fileHandle, INDEX_HEADER_ARRAY_HEADER_PAGE_IDX, 0 /* numElements */);
    pSlots = std::make_unique<InMemDiskArrayBuilder<Slot<common::ku_string_t>>>(
        *fileHandle, P_SLOTS_HEADER_PAGE_IDX, 0 /* numElements */);
    // Reserve a slot for oSlots, which is always skipped, as we treat slot idx 0 as NULL.
    oSlots = std::make_unique<InMemDiskArrayBuilder<Slot<common::ku_string_t>>>(
        *fileHandle, O_SLOTS_HEADER_PAGE_IDX, 1 /* numElements */);
    allocatePSlots(2);
    if (keyDataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        inMemOverflowFile =
            std::make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(fName));
    }
    //keyInsertFunc = InMemHashIndexUtils::initializeInsertFunc(indexHeader->keyDataTypeID);
    //keyEqualsFunc = InMemHashIndexUtils::initializeEqualsFunc(indexHeader->keyDataTypeID);
}

void HashIndexBuilderString::bulkReserve(uint32_t numEntries_) {
    slot_id_t numRequiredEntries = getNumRequiredEntries(numEntries.load(), numEntries_);
    // Build from scratch.
    auto numRequiredSlots = (numRequiredEntries + HashIndexConstants::SLOT_CAPACITY - 1) /
                            HashIndexConstants::SLOT_CAPACITY;
    auto numSlotsOfCurrentLevel = 1 << indexHeader->currentLevel;
    while ((numSlotsOfCurrentLevel << 1) < numRequiredSlots) {
        indexHeader->incrementLevel();
        numSlotsOfCurrentLevel = numSlotsOfCurrentLevel << 1;
    }
    if (numRequiredSlots > numSlotsOfCurrentLevel) {
        indexHeader->nextSplitSlotId = numRequiredSlots - numSlotsOfCurrentLevel;
    }
    allocatePSlots(numRequiredSlots);
    allocateOSlots(numRequiredSlots/20);
}

bool HashIndexBuilderString::appendInternal(const uint8_t* key, offset_t value) {
    uint8_t tag = 0;
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(*indexHeader, key, &tag), SlotType::PRIMARY};
    auto currentSlotInfo = pSlotInfo;

    Slot<common::ku_string_t>* primarySlot = getSlot(currentSlotInfo);
    Slot<common::ku_string_t>* currentSlot = nullptr;
    
    primarySlot->header.spinLock();
    if (lookupOrExistsInSlotWithoutLock<false /* exists */>(primarySlot, key, tag)) {
        // Key already exists. No append is allowed.
            primarySlot->header.unlock();
            return false;
    }
    currentSlot = primarySlot;
    if (currentSlot->header.numEntries == HashIndexConstants::SLOT_CAPACITY) {
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;

        while (currentSlotInfo.slotId != 0) {

            oSlotsSharedMutex.lock_shared();
            currentSlot = getSlot(currentSlotInfo);
            oSlotsSharedMutex.unlock_shared();

            if (lookupOrExistsInSlotWithoutLock<false /* exists */>(currentSlot, key, tag)) {
                // Key already exists. No append is allowed.
                return false;
            }
            if (currentSlot->header.numEntries < HashIndexConstants::SLOT_CAPACITY) {
                break;
            }
            currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        }
    }

    if (currentSlot->header.numEntries == HashIndexConstants::SLOT_CAPACITY) {
        auto ovfSlotId = getAOSlot();
        currentSlot->header.nextOvfSlotId = ovfSlotId;

        oSlotsSharedMutex.lock_shared();
        currentSlot = getSlot(SlotInfo{ovfSlotId, SlotType::OVF});
        oSlotsSharedMutex.unlock_shared();
    }

    insertToSlotWithoutLock(currentSlot, key, tag, value);
    
    primarySlot->header.unlock();
    numEntries.fetch_add(1);
    return true;
}

bool HashIndexBuilderString::lookupInternalWithoutLock(const uint8_t* key, offset_t& result) {
    uint8_t tag = 0;
    SlotInfo pSlotInfo{getPrimarySlotIdForKey(*indexHeader, key, &tag), SlotType::PRIMARY};
    SlotInfo currentSlotInfo = pSlotInfo;
    Slot<common::ku_string_t>* currentSlot;
    while (currentSlotInfo.slotType == SlotType::PRIMARY || currentSlotInfo.slotId != 0) {
        currentSlot = getSlot(currentSlotInfo);
        if (lookupOrExistsInSlotWithoutLock<true /* lookup */>(currentSlot, key, tag, &result)) {
            return true;
        }
        currentSlotInfo.slotId = currentSlot->header.nextOvfSlotId;
        currentSlotInfo.slotType = SlotType::OVF;
    }
    return false;
}

uint32_t HashIndexBuilderString::allocatePSlots(uint32_t numSlotsToAllocate) {
    auto oldNumSlots = pSlots->getNumElements();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    pSlots->resize(newNumSlots, true /* setToZero */);
    for (uint64_t i = oldNumSlots; i < newNumSlots; i++) {
        pSlots->operator[](i) = Slot<common::ku_string_t >();
    }
    return oldNumSlots;
}

uint32_t HashIndexBuilderString::allocateOSlots(uint32_t numSlotsToAllocate) {
    oSlotsSharedMutex.lock();
    auto oldNumSlots = oSlots->getNumElements();
    auto newNumSlots = oldNumSlots + numSlotsToAllocate;
    oSlots->resize(newNumSlots, true /* setToZero */);
    oSlotCapacity.store(newNumSlots);
    oSlotsSharedMutex.unlock();
    return oldNumSlots;
}

void HashIndexBuilderString::resizeOSlot() {
    if (emptyOSlotID.load() >= oSlotCapacity.load()) {
        oSlotsSharedMutex.lock();
        auto oldNumSlots = oSlots->getNumElements();
        auto addNewSlots = oldNumSlots / 2 + 1;
        oSlots->resize(oldNumSlots + addNewSlots, true /* setToZero */);
        oSlotCapacity.fetch_add(addNewSlots);
        oSlotsSharedMutex.unlock();
    }
}

Slot<common::ku_string_t>* HashIndexBuilderString::getSlot(const SlotInfo& slotInfo) {
    if (slotInfo.slotType == SlotType::PRIMARY) {
        return &pSlots->operator[](slotInfo.slotId);
    } else {
        return &oSlots->operator[](slotInfo.slotId);
    }
}

uint32_t HashIndexBuilderString::getAOSlot() {
    slot_id_t oSlotID = emptyOSlotID.fetch_add(1);
    if (oSlotID >= oSlotCapacity.load()) {
        resizeOSlot();
    }
    return oSlotID;
}

template<bool IS_LOOKUP>
bool HashIndexBuilderString::lookupOrExistsInSlotWithoutLock(
    Slot<common::ku_string_t>* slot, const uint8_t* key, const uint8_t tag, offset_t* result) {
    // increase capacity < 16
    bool loopCon[HashIndexConstants::SLOT_CAPACITY];
    for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
        loopCon[entryPos] = slot->header.isEntryValid(entryPos) && 
                            tag == slot->header.getPartialHash(entryPos);
    }
    for (auto entryPos = 0u; entryPos < HashIndexConstants::SLOT_CAPACITY; entryPos++) {
        auto& entry = slot->entries[entryPos];
        if (loopCon[entryPos] && InMemHashIndexUtils::equalsFuncForString(key, entry.data, inMemOverflowFile.get())) {
            if constexpr (IS_LOOKUP) {
                memcpy(result, entry.data + indexHeader->numBytesPerKey, sizeof(offset_t));
            }
            return true;
        }
    }
    return false;
}

void HashIndexBuilderString::insertToSlotWithoutLock(
    Slot<common::ku_string_t>* slot, const uint8_t* key, const uint8_t tag, offset_t value) {
    uint8_t entryPos = slot->header.numEntries;
    InMemHashIndexUtils::insertFuncForString(key, value, slot->entries[entryPos].data, inMemOverflowFile.get());
    slot->header.setEntryValid(entryPos);
    slot->header.setPartialHash(entryPos, tag);
    slot->header.numEntries++;
}

void HashIndexBuilderString::flush() {
    indexHeader->numEntries = numEntries.load();
    headerArray->resize(1, true /* setToZero */);
    headerArray->operator[](0) = *indexHeader;
    headerArray->saveToDisk();
    pSlots->saveToDisk();
    oSlots->saveToDisk();
    if (indexHeader->keyDataTypeID == LogicalTypeID::STRING) {
        inMemOverflowFile->flush();
    }
}

} // namespace storage
} // namespace kuzu
