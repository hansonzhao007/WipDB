// Copyright (c) 2018 Zhao, Xingsheng <xingshengzhao@gmail.com>
// All rights reserved. Use of this source code is governed by a 
// BSD-style license that can be found in the LICENSE file. 


#ifndef KV_DB_H_
#define KV_DB_H_

#include <string>
#include <vector>
#include <set>
#include <deque>
#include "kv/db.h"
#include "db/kv_iter.h"
#include "db/memtable.h"
#include "db/dbformat.h"
#include "db/write_thread.h"
#include "util/skiplist.h"
#include "kv/options.h"
#include "kv/slice.h"
#include "db/db_impl.h"
#include "db/bucket.h"
#include "util/hpblock.h"
#include "db/versionkv.h"

namespace kv {
class KVInserter;

struct MergeArgs {
    KV* kv;
    Bucket* bucket;
};

struct LOG{
    uint64_t number;
    uint64_t biggest_sequence;
    explicit LOG(uint64_t n, uint64_t seq): number(n), biggest_sequence(seq) {};
};

class KV { 


public:
    static Status Open(Options& options,
            const std::string& name,
            vector<std::string>& pivot, KV** dbptr, bool is_huge = false);
        // Build a mem array using existing key distribution info
    bool RestorePivots(std::string path, std::vector<std::string>&);
    
    Status BuildBuckets(vector<std::string>& pivot);

    Status Put(const WriteOptions&, const Slice& key, const Slice& value);
    Status Delete(const WriteOptions&, const Slice& key);
    Status Get(const ReadOptions& options,
                        const Slice& key,
                        std::string* value);

    Status Write(const WriteOptions& options, WriteBatch* updates);
    void PrintStats(const char* key);
    void CompactRange(int bucket_no);
    // Create a new KV object that will use "cmp" for comparing keys
    KV(const Options& option, const std::string dbname);
    ~KV();
    KVIter* NewIterator(const ReadOptions& options);

    void PrintPivots();
    friend Status DestroyKV(KV* kv);
    const vector<Bucket*>& GetBuckets() {return versions_->current()->buckets_;}
    void DeleteObsoleteLogs();

    uint64_t NewFileNumber() {
        next_file_number_.fetch_add(1, std::memory_order_relaxed);
        return next_file_number_.load(std::memory_order_relaxed);
    }

    bool GetProperty(const Slice& property, std::string* value);
private:
    friend class KVInserter;
    friend class DBImpl;
    struct Writer;


    static void BGWorkMerge(void* arg);
    Status RecoverLogFile(uint64_t log_number);

    
    void SavePivots(std::string path);
    void Merge(Bucket* b);
    Status SplitBucket(Bucket* n);

    // Constant after construction
    Env* const env_;
    EnvOptions env_options_;
    Options options_;  // options_.comparator == &internal_comparator_
    const std::string dbname_;
    const std::string logpath_;
    bool is_huge_;

    // State below is protected by mutex_
    port::Mutex version_lock_;
    WritableFile* logfile_;
    std::atomic<uint64_t> logfile_number_;
    log::Writer* log_;
    std::deque<LOG> old_logs_;
    uint64_t log_number_min_; // the min log that is still valid
    // The last seq visible to reads. It normally indicates the last sequence in
    // the memtable but when using two write queues it could also indicate the
    // last sequence in the WAL visible to reads.
    std::atomic<uint64_t> last_sequence_;
    std::atomic<uint64_t> next_file_number_;

    std::atomic<int64_t> split_work_;

    WriteThread write_thread_;
    WriteBatch tmp_batch_;

    VersionSetKV* versions_;
    
    // No copying allowed
    KV(const KV&);
    void operator=(const KV&);


    // Return the last sequence number.
    uint64_t LastSequence() const {
        return last_sequence_.load(std::memory_order_acquire);
    }

    // Set the last sequence number to s.
    void SetLastSequence(uint64_t s) {
        assert(s >= last_sequence_);
        last_sequence_.store(s, std::memory_order_release);
    }


    uint64_t NextLogNumber() {
        logfile_number_.fetch_add(1, std::memory_order_relaxed);
        return logfile_number_.load(std::memory_order_relaxed);
    }
    
    uint64_t LogNumber() {
        return logfile_number_.load(std::memory_order_relaxed);
    }

    void SetLogNumber(uint64_t s) {
        logfile_number_.store(s, std::memory_order_release);
    }

    uint64_t FileNumber() {
        return next_file_number_.load(std::memory_order_acq_rel);
    }
    void SetFileNumber(uint64_t s) {
        next_file_number_.store(s, std::memory_order_acq_rel);
    }
   

    Status WriteToWAL(const WriteThread::WriteGroup& write_group, WriteBatch*& merged_batch);
    bool ShouldKeepLog(uint64_t log_number);
};

Status DestroyKV(const std::string& dbname, const Options& options);
}  // namespace kv

#endif  // KV_DB_H_
