#ifndef KV_DB_ITER_H_
#define KV_DB_ITER_H_

#include <stdint.h>
#include <vector>
#include "db/bucket.h"
#include "db/dbformat.h"

namespace kv {

struct IterWrapper {
  Iterator* iter = nullptr;
  int bucket_num = -1;
};


class KVIter {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction {
    kForward,
    kReverse
  };

    KVIter(KV* db);
    ~KVIter();
    bool Valid() const { return valid_; }
    Slice key() const {
    assert(valid_);
    return (iter_.iter->key());
  }
   Slice value() const {
    assert(valid_);
    return iter_.iter->value();
  }
   Status status() const {
    if (status_.ok()) {
      return iter_.iter->status();
    } else {
      return status_;
    }
  }

   void Next();
   void Prev();
   void Seek(const Slice& target);
   void SeekToFirst();
   void SeekToLast();

 private:
  
  KV* db_;
  IterWrapper  iter_;
  
  const std::vector<Bucket*>& buckets_;
  Status status_;
  std::string saved_key_;     // == current key when direction_==kReverse
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Arena arena_;
  // No copying allowed
  KVIter(const KVIter&);
  void operator=(const KVIter&);
};

KVIter* NewKVIterator(KV* db);

}  // 

#endif 
