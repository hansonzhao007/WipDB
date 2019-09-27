#pragma once
#include <cstring>

#include "kv/slice.h"
#include "util/hash.h"
#include "util/murmurhash.h"
#include "util/coding.h"
#include "kv/memtablerep.h"

// Fast HashTable version 2
namespace kv{

// ===========  Slot: 8B  ============
// |----- 2B ------|------- 6B ------|
// |- partial_key -|- entry address -|

// partial_key 
// | -- 3 bit -- | -- 13 bit -- |
// |  |  X   X   |  kick off bucket space | 
//   |
//  used to indicate whether this slot is used or not
//  1: used
//  0: empty

const int kHashSlotNum = 8;
const int kHashBucketSize = 8192;  // 2 ^ 13

union HashSlot
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


// HashTable:
// BitMap + Buckets
// BitMap: every bucket has 1B bitmap

class HashHandler {
public:
    virtual void Action(KeyHandle entry) = 0;
};

// currently, hashtable support 8192 buckets, every bucket has 8 slot, so totally 65536 slots
class HashTable {
public:

    explicit HashTable();
    ~HashTable();
    class IterHandler: public HashHandler{
    public:
        MemTableRep* table_;
        explicit IterHandler(MemTableRep* table){
            assert(table != nullptr);
            table_ = table;
        };

        void Action(KeyHandle entry) override {
            table_->InsertKey(entry);
            // Slice key = FastHashTable::DecodeKeyFromEntry((const char*)entry);
            // Slice value = FastHashTable::DecodeValueFromEntry((const char*)entry);
            // printf("key: %s - value: %s\n", key.ToString().c_str(), value.ToString().c_str());
        }
    };

    void Iterate(HashHandler* handler);

    void Reset();

    void Clear();

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

    inline bool IsEmpty(uint32_t bi, uint8_t si) {
        assert(bi < kHashBucketSize);
        assert(si < kHashSlotNum);
        return (slots_[bi * kHashSlotNum + si].partial_key[3] & (1 << 15)) == 0;
    }

    inline bool IsPartialKeyEqual(uint32_t bi, uint8_t si, uint16_t partial_key) {
        assert(si < kHashSlotNum);
        return slots_[bi * kHashSlotNum + si].partial_key[3] == partial_key;
    }

    inline uint64_t Hash(const Slice& key) {
        return MurmurHash64A(key.data(), key.size(), 0xC3D2E1F0);
    }

    const int bucket_size_;
    HashSlot* slots_; // bucket_size * kHashSlotNum  slot
    uint32_t count_;
};

} // end of kv