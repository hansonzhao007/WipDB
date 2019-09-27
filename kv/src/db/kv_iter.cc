#include "db/kv_iter.h"

#include "db/filename.h"
#include "db/db_impl.h"
#include "db/dbformat.h"

#include "kv/kv.h"
#include "kv/env.h"
#include "kv/iterator.h"
#include "port/port.h"
// #include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"


namespace kv {

KVIter::KVIter(KV* db)
      : db_(db),
        direction_(kForward),
        valid_(false),
        arena_(nullptr),
        buckets_(db->GetBuckets()) {
          // printf("KVIter create\n");
  }

KVIter::~KVIter() {
    // printf("KVIter delete\n");
    if (iter_.iter) {
      delete iter_.iter;
    }
  }
void KVIter::Next() {
  static ReadOptions roptions;
  assert(valid_);
  direction_ = kForward;
  {
    iter_.iter->Next();
    // reach the end of current bucket, switch to next usable bucket if possible
    while(!iter_.iter->Valid() && (uint32_t)iter_.bucket_num < buckets_.size() - 1) {
      delete iter_.iter;
      iter_.iter = buckets_[++iter_.bucket_num]->db->NewIterator(roptions, &arena_);
      iter_.iter->SeekToFirst();
    }

    if(!iter_.iter->Valid()) {
      valid_ = false;
    }
    else {
      valid_ = true;
    }
  }

}


void KVIter::Prev() {
  static ReadOptions roptions;
  assert(valid_);
  direction_ = kReverse;
  {
    iter_.iter->Prev();
    // reach the beginning of current bucket, switch to previous bucket if possible
    while(!iter_.iter->Valid() && iter_.bucket_num > 0) {
      delete iter_.iter;
      iter_.iter = buckets_[--iter_.bucket_num]->db->NewIterator(roptions, &arena_);
      iter_.iter->SeekToLast();
    }
    if(!iter_.iter->Valid()) {
      valid_ = false;
    }
    else {
      valid_ = true;
    }
  }
}

void KVIter::Seek(const Slice& target) {
  static ReadOptions roptions;
  int ni = Bucket::lower_bound(buckets_, target);
  Bucket* n = buckets_[ni];
  iter_.iter = n->db->NewIterator(roptions, &arena_);
  iter_.bucket_num = ni;

  direction_ = kForward;
  
  iter_.iter->Seek(target);
  if (iter_.iter->Valid()) {
    valid_ = true;
  } else {
    if (n->bucket_old != nullptr) {
      delete iter_.iter;
      iter_.iter = n->bucket_old->db->NewIterator(roptions, &arena_);
      iter_.iter->Seek(target);
      if (iter_.iter->Valid()) {
        valid_ = true;
      }
      else {
        valid_ = false;
      }
    }
    else {
      valid_ = false;
    }
  }
}

void KVIter::SeekToFirst() {
  // find the first valid iter
  static ReadOptions roptions;
  direction_ = kForward;
  for(uint32_t i = 0; i < buckets_.size(); ++i) {
    Iterator* iter = buckets_[i]->db->NewIterator(roptions, &arena_);
    iter->SeekToFirst();
    if(iter->Valid()) {
      valid_ = true;
      iter_.iter = iter;
      iter_.bucket_num = i;
      return;
    }
    delete iter;
  }
  valid_ = false;
}

void KVIter::SeekToLast() {
  // 
  static ReadOptions roptions;
  direction_ = kReverse;
  for(int i = buckets_.size() - 1; i >= 0; --i) {
    Iterator* iter = buckets_[i]->db->NewIterator(roptions, &arena_);
    iter->SeekToLast();
    if(iter->Valid()) {
      valid_ = true;
      iter_.iter = iter;
      iter_.bucket_num = i;
      return;
    }
  }
  valid_ = false;
}

KVIter* NewKVIterator(KV* db) {
  return new KVIter(db);
}

}  // namespace leveldb
