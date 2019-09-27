// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "kv/db.h"

#include "kv/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "kv/filter_policy.h"
#include "db/bucket.h"
#include "util/arena.h"
#include "db/memtable.h"
#include "util/hash_table.h"
// #define LOGFILE
namespace kv {
class KV;
class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname, const std::string& store_name ="");
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value, uint64_t seq = 0);
  virtual Status Delete(const WriteOptions&, const Slice& key, uint64_t seq = 0);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates, uint64_t seq = 0);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);                
  virtual Iterator* NewIterator(const ReadOptions&, Arena* arena);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);

  virtual void AddFileToLastLevel(FileMetaData* meta);
  virtual int64_t TotalSize();
  // Extra methods (for testing) that are not in the public DB interface

  
  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();
  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  friend class KV;
  struct CompactionState;
  struct Writer;

  struct CompactionArg {
    // caller retains ownership of `db`.
    DBImpl* db;
    // background compaction takes ownership of `prepicked_compaction`.
    int level;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed, Arena* arena);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest, bool force_new = false)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
//   void DeleteObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void DeleteObsoleteFilesKV() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    
  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
//   void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CompactMemTableKV() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

//   Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
//                         VersionEdit* edit, SequenceNumber* max_sequence)
//       EXCLUSIVE_LOCKS_REQUIRED(mutex_);

//   Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
//       EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status WriteLevel0TableKV(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
      
//   Status MakeRoomForWrite(bool force /* compact even if there is room? */)
//       EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status MakeRoomForWriteKV(bool force)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWorkSplit(void* db);
  static void BGWorkFlush(void* db);
  static void BGWorkCompaction(void* arg);
  static void BGWorkCompactionManual(void* db);
  void BackgroundCallFlush();
  void BackgroundCallCompaction(int level);
  void BackgroundCallCompactionManual();
  void BackgroundCallSplit();
  void BackgroundSplit();
//   void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void BackgroundCompaction(int level) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void BackgroundCompactionManual() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  

  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWorkKV(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoSplitWork(CompactionState* compact)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);
//   Status OpenCompactionOutputFile(CompactionState* compact);
  Status OpenCompactionOutputFileKV(CompactionState* compact);
//   Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status FinishCompactionOutputFileKV(CompactionState* compact, Iterator* input);
//   Status InstallCompactionResults(CompactionState* compact)
//       EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status InstallCompactionResultsKV(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);


  
  
  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  MemOptions mem_options_;
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;
  const std::string store_name_;

  int flush_size_;
  
  HugePageBlock* hpblock_;
  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // // Lock over the persistent DB state.  Non-null iff successfully acquired.
  // FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_; // 表示 db 正被 shutting down
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::deque<HashTable*> hashtable_free_;
  // std::deque<MemTable*> flush_queue_ GUARDED_BY(mutex_); // buffered imm table

  port::AtomicPointer has_imm_;       // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  log::Writer* log_; // 和 mem_ 对应的 log 的 writer
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.
  Bucket* bucket_;
  KV* kv_;
  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);
  std::set<uint64_t> evicted_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_); // bg flush 
  bool background_compaction_scheduled_manual_ GUARDED_BY(mutex_); // bg compaction manual
  std::vector<bool> background_compaction_scheduled_level_ GUARDED_BY(mutex_); // bg compaction
  bool background_split_scheduled_ GUARDED_BY(mutex_); // bg split
  bool background_split_mem_to_imm_ GUARDED_BY(mutex_); // bg force mem to imm, doing in split
  bool background_merge_scheduled_ GUARDED_BY(mutex_);
    // A value of > 0 temporarily disables scheduling of background compaction
  std::atomic<bool> has_flush_wait_;
  std::atomic<bool> has_too_many_level0_files_;
  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;   // null means beginning of key range
    const InternalKey* end;     // null means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);
  bool query_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  CompactionMode compaction_mode_;
  
  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

  int append_level_;
  int append_max_file_num_;
  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
