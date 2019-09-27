// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef KV_MEMTPAGE_H_
#define KV_MEMTPAGE_H_

#include <string>
#include <memory>
#include <vector>

#include "util/skiplist.h"
#include "db/dbformat.h"
#include "kv/comparator.h"
#include "kv/status.h"
#include "kv/iterator.h"
#include "util/hpblock.h"
#include "kv/memtablerep.h"
#include "memtable/hash_cuckoo_rep.h"
#include "memtable/hash_skiplist_rep.h"
#include "db/memtable.h"
#include "kv/slice.h"
#include "util/hash.h"
#include "sparsepp/spp.h"
#include "util/hash_table.h"
#include "kv/options.h"

using spp::sparse_hash_map;
using std::unique_ptr;
using std::shared_ptr;



namespace kv {
    
class InternalKeyComparator;
class MemTableIterator;

struct SliceHash
{
    size_t operator()(const Slice& k) const
    {
        // Compute individual hash values for first,
        // second and third and combine them using XOR
        // and bit shifting:
        return (size_t) Hash(k.data(), k.size(), 0);
    }
};

struct ArrayNode {
    ArrayNode(const Slice& _key, KeyHandle _entry) {
        key = _key;
        entry = _entry;
    }
    Slice key;
    KeyHandle entry;
};


struct MemOptions {
    MemTableRepFactory* memtable_factory;
    const Options& options;
    bool append_mode;
    bool skiplist_insertion;
    explicit MemOptions(const Options& raw_option): options(raw_option) {
        memtable_factory = new SkipListFactory();
        append_mode = false;
        skiplist_insertion = false;
    }
    
};


class MemTable {
public:
    bool double_flush_size_once_ = false;
    int flush_size_;
    struct KeyComparator : public MemTableRep::KeyComparator {
        const InternalKeyComparator comparator;
        explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
        virtual int operator()(const char* a, const char* b) const override;
        virtual int operator()(const char* prefix_len_key, const DecodedType& key) const override;
    };


    
    struct ByArrayNode {
    const InternalKeyComparator comparator;
    ByArrayNode(): comparator(Options().comparator){}
    bool operator() (const ArrayNode& a, const ArrayNode& b) {
        Slice ua(a.key.data(), a.key.size() - 8);
        Slice ub(b.key.data(), b.key.size() - 8);
        int res = ua.compare(ub);
        if (res == 0) {
            const uint64_t anum = DecodeFixed64(ua.data() + ua.size()); // seq << 8 | kValueType
            const uint64_t bnum = DecodeFixed64(ub.data() + ub.size()); // seq << 8 | kValueType
            if (anum > bnum) {
                res = -1;
            }
            else if (anum < bnum) {
                res = +1;
            }
        }
        return res < 0;
        // printf("user key. a(%d)#%d: %s -- b(%d)#%d: %s\n", 
        // ua.size(), 
        // aseq,
        // ua.ToString().c_str(), 
        // ub.size(), 
        // bseq,
        // ub.ToString().c_str());
    }
    };

    explicit MemTable(const InternalKeyComparator& cmp, HugePageBlock* hpblock, const MemOptions& mem_options, int flush_size = 1048576, HashTable* hashtable = nullptr);

    size_t ApproximateMemoryUsage();
    
    size_t ActualMemoryUsage();
    // size_t MemoryRemain();

    void Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value);
   
    bool Get(const LookupKey& key, std::string* value, Status* s);


    void SortTable();
    
    // The caller must ensure that the underlying MemTable remains live
    // while the returned iterator is live.  The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey in the
    // db/format.{h,cc} module.
    Iterator* NewIterator(const ReadOptions& read_options, Arena* arena);
    // Increase reference count.
    void Ref() { ++refs_; }

    // Drop reference count.  Delete if no more references exist.
    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        if (refs_ <= 0) {
        delete this;
        }
    }
    
    const InternalKeyComparator& GetInternalKeyComparator() const {
        return comparator_.comparator;
    }
    
    HashTable* GetHashTable() {
        return hash_table_;
    }

private: 
    ~MemTable();  // Private since only Unref() should be used to delete it
    
    friend class MemTableIterator;
    friend class MemArrayIterator;
    friend class MemTableBackwardIterator;
    
    typedef SkipList<const char*, KeyComparator> Table;

    const MemOptions mem_options_;
    KeyComparator comparator_;
    Arena arena_;
    // Table table_;
    std::vector<ArrayNode> array_;
    unique_ptr<MemTableRep> table_;
    HashTable* hash_table_;
    
    std::vector<ArrayNode> pool_;
    bool sorted_;
    Logger* info_log_;
    int refs_;
    port::AtomicPointer actual_memory_usage_;
    
    // No copying allowed
    MemTable(const MemTable&);
    void operator=(const MemTable&);
};

extern const char* EncodeKey(std::string* scratch, const Slice& target);

}  // namespace kv

#endif  // KV_MemTable_H_
