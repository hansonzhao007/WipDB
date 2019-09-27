#include "hash_table.h"

namespace kv {

HashTable::HashTable():
        bucket_size_(kHashBucketSize) {
        slots_   = new HashSlot[bucket_size_ * kHashSlotNum]();
        count_ = 0;
    }
HashTable::~HashTable(){
    delete [] slots_;
}

void HashTable::Iterate(HashHandler* handler) {
    if (count_ == 0) return;
    for (int bi = 0; bi < bucket_size_; ++bi) {
        for (int si = 0; si < kHashSlotNum; ++si) {
            if (!IsEmpty(bi, si)) {
                auto entry = (KeyHandle) slots_[bi * kHashSlotNum + si].entry;
                entry = (char*)((uint64_t)entry & 0xffffffffffff);
                handler->Action(entry);
            }
        }
    }
}

// set bitmap to 0
void HashTable::Reset() {
    memset(slots_, 0, sizeof(HashSlot) * bucket_size_ * kHashSlotNum);
    count_ = 0;
}

// set all content to 0
void HashTable::Clear() {
    memset(slots_, 0, sizeof(HashSlot) * bucket_size_ * kHashSlotNum);
    count_ = 0;
}


bool HashTable::Put(const Slice& key, const char* entry) {
    uint64_t hash = Hash(key);
    uint32_t bi1 = (hash & 0x1FFF);            // low  13 bit
    uint32_t bi2 = ((hash >> 32) & 0x1FFF);    // low  13 bit of the high 16 bit
    uint16_t partial_key1 = bi2 | 0x8000;      // set high bit to 1, partial_key save the kick off position
    uint16_t partial_key2 = bi1 | 0x8000;      // set high bit to 1, partial_key save the kick off position
    for (int si = 0; si < kHashSlotNum; ++si) {
        if (IsEmpty(bi1, si)) {
            slots_[bi1 * kHashSlotNum + si].entry = entry;
            slots_[bi1 * kHashSlotNum + si].partial_key[3] = partial_key1;
            ++count_;
            return true;
        }
    }
    return false;
}

bool HashTable::Get(const Slice& key, const char*& entry) {
    uint64_t hash = Hash(key);
    uint32_t bi1 = (hash & 0x1FFF);            // low  13 bit
    uint32_t bi2 = ((hash >> 32) & 0x1FFF);    // low  13 bit of the high 16 bit
    uint16_t partial_key1 = bi2 | 0x8000;      // set high bit to 1, partial_key save the kick off position
    uint16_t partial_key2 = bi1 | 0x8000;      // set high bit to 1, partial_key save the kick off position

    for (int si = 0; si < kHashSlotNum; ++si) {
        if (!IsEmpty(bi1, si) && IsPartialKeyEqual(bi1, si, partial_key1)) {
            Slice entry_key = DecodeKeyFromEntry(slots_[bi1 * kHashSlotNum + si].entry);
            if (entry_key.compare(key) == 0) {
                entry = slots_[bi1 * kHashSlotNum + si].entry;
                entry = (char*)((uint64_t)entry & 0xffffffffffff); // mask partial key part
                return true;
            }
        }
    }
    return false;
}

}// end of kv