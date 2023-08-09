#pragma once

#include <cstdint>
#include <atomic>
#include <memory>
#include <iostream>
#include <mutex>

#include "common/constants.h"
#include "common/types/internal_id_t.h"
#include "common/types/ku_string.h"
#include "storage/index/hash_index_utils.h"

namespace kuzu {
namespace storage {

using entry_pos_t = uint8_t;
using slot_id_t = uint64_t;

class SlotHeader {
public:
    static const entry_pos_t INVALID_ENTRY_POS = UINT8_MAX;

    SlotHeader() : numEntries{0}, partialHash{0}, validityMask{0}, nextOvfSlotId{0} {
        slotLockState = std::make_shared<std::atomic<bool>>(false);
    }

    void reset() {
        numEntries = 0;
        validityMask = 0;
        partialHash = 0;
        nextOvfSlotId = 0;
        slotLockState = std::make_shared<std::atomic<bool>>(false);
    }

    inline bool isEntryValid(__uint128_t entryPos) const {
        return validityMask & ((__uint128_t)1 << entryPos);
    }
    inline void setEntryValid(entry_pos_t entryPos) { validityMask |= ((__uint128_t)1 << entryPos); }
    inline void setEntryInvalid(entry_pos_t entryPos) {
        validityMask &= ~((__uint128_t)1 << entryPos);
    }

    inline void setPartialHash(entry_pos_t entryPos, const uint8_t tag) {
        __uint128_t partial = tag;
        partial = partial << 8*entryPos;
        partialHash |= partial;

    }

    inline uint8_t getPartialHash(entry_pos_t entryPos) {

        uint8_t extracted_partial = 0;
        uint8_t shift_bits = (8*entryPos);

        extracted_partial = (partialHash >> shift_bits) & 0xFF;
        return extracted_partial;
    }

    inline bool isPartialHashMatch(entry_pos_t entryPos, kuzu::common::hash_t hash) {
        uint8_t extracted_partial = getPartialHash(entryPos);
        return HashIndexUtils::compute_tag(hash) == extracted_partial;
    }

    inline void spinLock() {
        while (true) {
            bool expected = false;
            if (slotLockState->compare_exchange_strong(expected, true)){
                return;
            }
        }
    }

    inline void unlock() {
        assert(slotLockState->load());
        slotLockState->store(false, std::memory_order_release);
    }
    
public:
    __uint128_t numEntries;
    __uint128_t partialHash;
    __uint128_t validityMask;
    slot_id_t nextOvfSlotId;
    std::shared_ptr<std::atomic<bool>> slotLockState;
};

template<typename T>
struct SlotEntry {
    uint8_t data[sizeof(T) + sizeof(common::offset_t)];
};

template<typename T>
struct Slot {
    SlotHeader header;
    SlotEntry<T> entries[common::HashIndexConstants::SLOT_CAPACITY];

    Slot() {
        header = SlotHeader();
    }

};

} // namespace storage
} // namespace kuzu
