#include "hashtable.h"

namespace kv {


FastHashTable::FastHashTable():
        bucket_size_(kBucketSize) {
        slots_   = new FastHashSlot[bucket_size_ * kBucketSlotNum];
        bitmaps_ = new char[bucket_size_]();
        count_ = 0;
    }
FastHashTable::~FastHashTable(){
    delete [] bitmaps_;
    delete [] slots_;
}

void FastHashTable::Iterate(IterHandler* handler) {
    if (count_ == 0) return;
    for (int bi = 0; bi < bucket_size_; ++bi) {
        for (int si = 0; si < kBucketSlotNum; ++si) {
            if (ValidSlot(bi, si)) {
                auto entry = (KeyHandle) slots_[bi * kBucketSlotNum + si].entry;
                entry = (char*)((uint64_t)entry & 0xffffffffffff);
                handler->Add(entry);
            }
        }
    }
}

// set bitmap to 0
void FastHashTable::Reset() {
    memset(bitmaps_, 0, sizeof(char) * bucket_size_);
    count_ = 0;
}

// set all content to 0
void FastHashTable::Clear() {
    memset(bitmaps_, 0, sizeof(char) * bucket_size_);
    memset(slots_, 0, sizeof(FastHashSlot) * bucket_size_ * kBucketSlotNum);
    count_ = 0;
}


    // partial_key will store alternative pos info. bi(13bit) + si(3bit)

bool FastHashTable::Put(const Slice& key, const char* entry) {
    uint64_t hash = Hash(key);
    uint32_t bi1 = (hash & 0x1FFF);            // low  13 bit
    uint32_t bi2 = ((hash >> 32) & 0x1FFF);    // low  13 bit of the high 16 bit
    uint8_t  si1 = (bi1 >> 16) & 0x07;
    uint8_t  si2 = (~si1) & 0x07;
    uint16_t partial_key1 = bi2 << 3 | si2;
    uint16_t partial_key2 = bi1 << 3 | si1;
    // try to insert to bucket 1 with partial_key1 (store location of bi2, si2)
    if (IsEmpty(bi1, si1) || TryKick(bi1, si1)) {
        slots_[bi1 * kBucketSlotNum + si1].entry = entry;
        slots_[bi1 * kBucketSlotNum + si1].partial_key[3] = partial_key1;
        SetBitMap(bi1, si1);
        ++count_;
        return true;
    } 
    // try to insert to bucket 2 with partial_key2 (store location of bi1, si1)
    // if bi2,si2 is occupyied, try to kick it to alternative, and then insert 
    else if (IsEmpty(bi2, si2) || TryKick(bi2, si2)) {
        slots_[bi2 * kBucketSlotNum + si2].entry = entry;
        slots_[bi2 * kBucketSlotNum + si2].partial_key[3] = partial_key2;
        SetBitMap(bi2, si2);
        ++count_;
        return true;
    }
    
    return false;
}

bool FastHashTable::Get(const Slice& key, const char*& entry) {
    uint64_t hash = Hash(key);
    uint32_t bi1 = (hash & 0x1FFF);            // low  13 bit
    uint32_t bi2 = ((hash >> 32) & 0x1FFF);    // low  13 bit of the high 16 bit
    uint8_t  si1 = (bi1 >> 16) & 0x07;
    uint8_t  si2 = (~si1) & 0x07;
    uint16_t partial_key1 = bi2 << 3 | si2;
    uint16_t partial_key2 = bi1 << 3 | si1;

    if (!IsEmpty(bi1, si1) && IsPartialKeyEqual(bi1, si1, partial_key1)) {
        Slice entry_key = DecodeKeyFromEntry(slots_[bi1 * kBucketSlotNum + si1].entry);
        if (entry_key.compare(key) == 0) {
            entry = slots_[bi1 * kBucketSlotNum + si1].entry;
            entry = (char*)((uint64_t)entry & 0xffffffffffff);
            return true;
        }
    }
    if (!IsEmpty(bi2, si2) && IsPartialKeyEqual(bi2, si2, partial_key2)) {
        Slice entry_key = DecodeKeyFromEntry(slots_[bi2 * kBucketSlotNum + si2].entry);
        if (entry_key.compare(key) == 0) {
            entry = slots_[bi2 * kBucketSlotNum + si2].entry;
            entry = (char*)((uint64_t)entry & 0xffffffffffff);
            return true;
        }
    }
    return false;
}

}// end of kv