// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "util/coding.h"
#include "kv/status.h"
#include "kv/iterator.h"
#include "kv/env.h"

#include <iostream>
namespace kv {


// Callback from MemTable::Get()
namespace {

struct Saver {
  Status* status;
  const LookupKey* key;
  bool* found_final_value;  // Is value set correctly? Used by KeyMayExist
  std::string* value;
  MemTable* mem;

};
}  // namespace

class ArrayHandler: public HashHandler {
public:
    explicit ArrayHandler (std::vector<ArrayNode>* _array) {
        array = _array;
    }
    void Action (KeyHandle entry) override {
        Slice internal_key = GetLengthPrefixedSlice((char*)entry);
        array->push_back(ArrayNode(internal_key, entry));
    }
private:
    std::vector<ArrayNode>* array;
};

MemTable::MemTable(const InternalKeyComparator& cmp, HugePageBlock* hpblock, const MemOptions& mem_options, int flush_size, HashTable* hashtable )
    : 
      mem_options_(mem_options),
      comparator_(cmp),
      arena_(hpblock),
      table_(mem_options_.memtable_factory->CreateMemTableRep(comparator_, &arena_, mem_options.options.prefix_extractor.get(), nullptr)),
      hash_table_(hashtable != nullptr ? hashtable : new HashTable()),
      sorted_(false),
      info_log_(mem_options.options.info_log),
      refs_(0),
      actual_memory_usage_(0),
      flush_size_(flush_size)
      {
        
}

MemTable::~MemTable() {
  assert(refs_ == 0);
  // we reset hash table when distroying mem table
  // Because mem deletion is done in background, so background deletion can reduce forground reset overhead
  hash_table_->Reset(); 

}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }
// size_t MemTable::MemoryRemain() {return arena_.MemoryRemain();}
size_t MemTable::ActualMemoryUsage() {return reinterpret_cast<uintptr_t>(actual_memory_usage_.NoBarrier_Load());}

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}


int MemTable::KeyComparator::operator()(const char* prefix_len_key,
                                        const KeyComparator::DecodedType& key)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.CompareKeySeq(a, key);
}

class MemTableIterator : public Iterator {
 public:
  MemTableIterator(const MemTable& mem, const ReadOptions& read_options,
                   Arena* arena)
      : comparator_(mem.comparator_),
        valid_(false),
        arena_mode_(arena != nullptr)
        {
    iter_ = mem.table_->GetIterator(arena);    
  }

  ~MemTableIterator() {
    if (arena_mode_) {
      iter_->~Iterator();
    } else {
      delete iter_;
    }
  }


  virtual bool Valid() const override { return valid_; }
  virtual void Seek(const Slice& k) override {
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
  }
  
  virtual void SeekToFirst() override {
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
  }
  virtual void SeekToLast() override {
    iter_->SeekToLast();
    valid_ = iter_->Valid();
  }
  virtual void Next() override {
    assert(Valid());
    iter_->Next();
    valid_ = iter_->Valid();
  }
  virtual void Prev() override {
    assert(Valid());
    iter_->Prev();
    valid_ = iter_->Valid();
  }
  virtual Slice key() const override {
    assert(Valid());
    return GetLengthPrefixedSlice(iter_->key());
  }
  virtual Slice value() const override {
    assert(Valid());
    Slice key_slice = GetLengthPrefixedSlice(iter_->key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const override { return Status::OK(); }

 private:
  const MemTable::KeyComparator comparator_;
  MemTableRep::Iterator* iter_;
  bool valid_;
  bool arena_mode_;

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};


class MemArrayIterator : public Iterator {
 public:
  MemArrayIterator(const MemTable& mem)
      : valid_(false)
        {
    begin_ = mem.array_.begin();
    end_   = mem.array_.end();
    iter_ = begin_;
  }

  ~MemArrayIterator() {
  }


  virtual bool Valid() const override { return valid_; }
  virtual void Seek(const Slice& k) override {
    auto iter = std::lower_bound(begin_, end_, ArrayNode(k, (KeyHandle)0), MemTable::ByArrayNode());
    valid_ = iter != end_;
  }
  
  virtual void SeekToFirst() override {
    iter_ = begin_;
    valid_ = iter_ != end_;
  }
  virtual void SeekToLast() override {
    iter_ = end_;
    valid_ = iter_ - begin_ >= 0;
  }
  virtual void Next() override {
    assert(Valid());
    iter_++;
    valid_ = iter_ != end_;
  }
  virtual void Prev() override {
    assert(Valid());
    iter_--;
    valid_ = iter_ - begin_ >= 0;
  }
  virtual Slice key() const override {
    assert(Valid());
    return iter_->key;
    // return GetLengthPrefixedSlice(iter_->key());
  }
  virtual Slice value() const override {
    assert(Valid());
    Slice key_slice = GetLengthPrefixedSlice((char*)iter_->entry);
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const override { return Status::OK(); }

 private:
  std::vector<ArrayNode>::const_iterator iter_;
  std::vector<ArrayNode>::const_iterator begin_;
  std::vector<ArrayNode>::const_iterator end_;
  bool valid_;


  // No copying allowed
  MemArrayIterator(const MemArrayIterator&);
  void operator=(const MemArrayIterator&);
};

Iterator* MemTable::NewIterator(const ReadOptions& read_options, Arena* arena) {
  if (mem_options_.skiplist_insertion) {
    assert(arena != nullptr);
    auto mem = arena->AllocateAligned(sizeof(MemTableIterator));
    return new MemTableIterator(*this, read_options, arena);
  }
  else {
    SortTable();
    return new MemArrayIterator(*this);
  }
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  uint32_t key_size = static_cast<uint32_t>(key.size());
  uint32_t val_size = static_cast<uint32_t>(value.size());
  uint32_t internal_key_size = key_size + 8;
  const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size + 
                               VarintLength(val_size) + val_size;
  char* buf = nullptr;
  KeyHandle handle = table_->Allocate(encoded_len, &buf);
  
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  Slice key_slice(p, key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  
  actual_memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(ActualMemoryUsage() + encoded_len));


  
  if (!mem_options_.skiplist_insertion) {
    // insert entry to fast hashtable
    if(!hash_table_->Put(key_slice, buf)) {
        // if hash collision happens, insert to backup pool
        // Log(info_log_, "MemTable hash collision, insert to backup pool.");
        pool_.push_back(ArrayNode(key_slice, handle));
    }
  }
  else {
    table_->InsertKey(handle);
  }
  
}


static bool SaveValue(void* arg, const char* entry) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  assert(s != nullptr);
  // entry format is:
  //    klength  varint32
  //    userkey  char[klength-8]
  //    tag      uint64
  //    vlength  varint32
  //    value    char[vlength]
  // Check that it belongs to same user key.  We do not check the
  // sequence number since the Seek() call above should have skipped
  // all entries with overly large sequence numbers.
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  if (s->mem->GetInternalKeyComparator().user_comparator()->Equal(
          Slice(key_ptr, key_length - 8), s->key->user_key())) {
    // Correct user key
    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    switch (static_cast<ValueType>(tag & 0xff))  {
      case kTypeValue: {
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        *(s->status) = Status::OK();
        if (s->value != nullptr) {
          s->value->assign(v.data(), v.size());
        }
        *(s->found_final_value) = true;
        return false;
      }
      case kTypeDeletion: {
        *(s->status) = Status::NotFound();
        *(s->found_final_value) = true;
        return false;
      }
      default:
        assert(false);
        return true;
    }
  }
  // s->state could be Corrupt, merge or notfound
  return false;
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
    bool found_final_value = false;
    Saver saver;
    saver.status = s;
    saver.found_final_value = &found_final_value;
    saver.key = &key;
    saver.value = value;
    saver.mem = this;

    Slice user_key = key.user_key();
    const char* entry = nullptr;

    if (mem_options_.skiplist_insertion) {
      table_->Get(key, &saver, SaveValue);
    }
    else {
      // search hashtable
      if (hash_table_->Get(user_key, entry)) {
        SaveValue(&saver, entry);
      }
      // search backup pool_ 
      else {
        for (auto& e : pool_) {
          if (e.key.compare(user_key) == 0) {
            SaveValue(&saver, (char*)e.entry);
          }
        }
      }

      // if we do sort during insertion, then partion of k-v is sorted and store in ...
      if (sorted_) {
        Slice internal_key = key.internal_key();
        auto iter = std::lower_bound(array_.begin(), array_.end(), ArrayNode(internal_key, (KeyHandle)entry), ByArrayNode());
        if (iter != array_.end()) {
          Slice iter_user_key(iter->key.data(), iter->key.size() - 8);
          int res = iter_user_key.compare(user_key);
          // user key is equal
          // const uint64_t iter_num = DecodeFixed64(iter_user_key.data() + iter_user_key.size()) >> 8; 
          // const uint64_t key_num  = DecodeFixed64(key.user_key().data() + key.user_key().size()) >> 8;
          if (res == 0) {
            // printf("Key#%d: %s. Iter key#%d: %s\n", 
            // key_num,
            // user_key.ToString().c_str(),
            // iter_num,
            // iter_user_key.ToString().c_str());
            SaveValue(&saver, (char*) iter->entry);
          }
          // else {
          //   printf("Sorted array search key not equal. Key#%d: %s. Iter key#%d: %s\n", 
          //   key_num,
          //   user_key.ToString().c_str(),
          //   iter_num,
          //   iter_user_key.ToString().c_str());
          // }
          
        } 
      }
    }
    return found_final_value;
}

void MemTable::SortTable() {
    // Log(info_log_, "Sort Table start. hashtable size: %d. pool size: %d", (int)hash_table_->Count(), (int)pool_.size());
    // Log(info_log_, "High Queue: %d. Low Queue: %d", Env::Default()->GetThreadPoolQueueLen(Env::Priority::HIGH), Env::Default()->GetThreadPoolQueueLen(Env::Priority::LOW));
    auto start = Env::Default()->NowMicros();
    // sort all entry to skiplist 
    sorted_ = true;
    // HashTable::IterHandler handler(table_.get());

    ArrayHandler array_handler(&array_);
    hash_table_->Iterate(&array_handler); // insert hashtable entry to array_
    
    // insert pool_ entry to array_
    for (auto& e : pool_) {
        Slice internal_key = GetLengthPrefixedSlice((char*)e.entry);
        array_.push_back(ArrayNode(internal_key, (char*)e.entry));
    }

    sort(array_.begin(), array_.end(), ByArrayNode()); // sort array_

    auto duration = Env::Default()->NowMicros() - start;

    // Log(info_log_, "Sort Table end. %llu us", (unsigned long long) duration);

    hash_table_->Reset();
    pool_.clear();
    
}



void MemTableRep::InsertConcurrently(KeyHandle /*handle*/) {
#ifndef ROCKSDB_LITE
  throw std::runtime_error("concurrent insert not supported");
#else
  abort();
#endif
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

KeyHandle MemTableRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}


void MemTableRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {
  auto iter = GetDynamicPrefixIterator();
  for (iter->Seek(k.internal_key(), k.memtable_key().data());
       iter->Valid() && callback_func(callback_args, iter->key());
       iter->Next()) {
  }
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, static_cast<uint32_t>(target.size()));
  scratch->append(target.data(), target.size());
  return scratch->data();
}


}  // namespace kv
