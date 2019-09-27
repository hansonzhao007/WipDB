#pragma once
#include <cstring>

#include "kv/slice.h"
#include "util/hash.h"
#include "util/murmurhash.h"
#include "util/coding.h"
#include "kv/memtablerep.h"

namespace kv{

// ===========  Slot: 8B  ============
// |----- 2B ------|------- 6B ------|
// |- partial_key -|- entry address -|

const int kBucketSlotNum = 8;
const int kBucketSize = 8192;  // 2 ^ 13

union FastHashSlot
{
    /* data */
    const char* entry;   // 6B
    uint16_t partial_key[4]; // in little endian, we need to overwrite partial_key[3]
};


// Bucket: 64B
// |- slot -|
// |- slot -|
// |- slot -|
// |- slot -|
// |- slot -|
// |- slot -|
// |- slot -|
// |- slot -|

// struct FastHashBucket {
//     FastHashSlot* slots;
// };

// HashTable:
// BitMap + Buckets
// BitMap: every bucket has 1B bitmap

// currently, hashtable support 8192 buckets, every bucket has 8 slot, so totally 65536 slots
class FastHashTable {
public:

    explicit FastHashTable();
    ~FastHashTable();
    class IterHandler{
    public:
        MemTableRep* table_;
        explicit IterHandler(MemTableRep* table){
            assert(table != nullptr);
            table_ = table;
        };

        void Add(KeyHandle entry) {
            table_->InsertKey(entry);
            // Slice key = FastHashTable::DecodeKeyFromEntry((const char*)entry);
            // Slice value = FastHashTable::DecodeValueFromEntry((const char*)entry);
            // printf("key: %s - value: %s\n", key.ToString().c_str(), value.ToString().c_str());
        }
    };

    void Iterate(IterHandler* handler);

    void Reset();

    void Clear();
    // partial_key will store alternative pos info. bi(13bit) + si(3bit)
    int Count() {
        return count_;
    }
    bool Put(const Slice& key, const char* entry);

    bool Get(const Slice& key, const char*& entry);
    
    static Slice DecodeKeyFromEntry(const char* entry) {
        char* addr = (char*)((uint64_t)entry & 0xffffffffffff);
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(addr, addr + 5, &key_length);
        return Slice(key_ptr, key_length - 8);
    }

    static Slice DecodeValueFromEntry(const char* entry) {
        char* addr = (char*)((uint64_t)entry & 0xffffffffffff);
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(addr, addr + 5, &key_length);
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        return v;
    }

private:
    inline bool ValidSlot(uint32_t bi, uint8_t si) {
        assert(bi < kBucketSize);
        assert(si < kBucketSlotNum);
        return bitmaps_[bi] & (1 << si);
    }
    inline bool TryKick(uint32_t bi, uint8_t si) {
        assert(bi < kBucketSize);
        assert(si < kBucketSlotNum);
        uint16_t partial_key = slots_[bi * kBucketSlotNum + si].partial_key[3];
        const char* entry = slots_[bi * kBucketSlotNum + si].entry;
        uint32_t alter_bi = partial_key >> 3;
        uint8_t  alter_si = partial_key & 0x07;
        if (IsEmpty(alter_bi, alter_si)) {
            // printf("Kick Success\n");
            uint16_t alter_partial_key = bi << 3 | si; // store the kicked position as partial key of alternative position
            SetBitMap(alter_bi, alter_si);
            slots_[alter_bi * kBucketSlotNum + alter_si].entry = entry;
            slots_[alter_bi * kBucketSlotNum + alter_si].partial_key[3] = alter_partial_key;
            ClearBitMap(bi, si);
            return true;
        }
        return false;
    }
    inline bool IsEmpty(uint32_t bi, uint8_t si) {
        assert(si < 8);
        return (bitmaps_[bi] & ( 1 << si)) == 0;
    }

    inline bool IsPartialKeyEqual(uint32_t bi, uint8_t si, uint16_t partial_key) {
        assert(si < kBucketSlotNum);
        return slots_[bi * kBucketSlotNum + si].partial_key[3] == partial_key;
    }

    // set to 1
    inline void SetBitMap(uint32_t bi, uint8_t si) {
        assert(si < kBucketSlotNum);
        bitmaps_[bi] |= (1 << si);
    }

    // set to 0
    inline void ClearBitMap(uint32_t bi, uint8_t si) {
        assert(si < kBucketSlotNum);
        uint8_t mask = ~(1 << si);
        bitmaps_[bi] &= mask;
    }

    
    inline uint64_t Hash(const Slice& key) {
        return MurmurHash64A(key.data(), key.size(), 0xC3D2E1F0);
    }

    const int bucket_size_;
    FastHashSlot* slots_; // bucket_size * kBucketSlotNum  slot
    char* bitmaps_; // bitmap set to 1 means current position is used
    uint32_t count_;
};

} // end of kv