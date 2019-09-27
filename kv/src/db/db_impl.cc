// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include "kv/kv.h"
#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include <cmath>
#include <sstream>
#include <unistd.h>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "kv/db.h"
#include "kv/env.h"
#include "kv/status.h"
#include "kv/table.h"
#include "kv/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
// #include "util/logging.h"
#include "util/mutexlock.h"
#include "util/hash.h"
#include "util/generator.h"
#include "util/file_reader_writer.h"


namespace kv {
using namespace config;

const int kNumNonTableCacheFiles = 10;
const int kBGWorkFlushTag = 10;
const int kBGWorkMergeAfterSplit = 20;
// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    uint64_t count;
    InternalKey smallest, largest, median;
    InternalKey one_quarter, three_quarter;
    InternalKey one_eighth, three_eighth, five_eighth, seven_eighth;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFileWriter* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    printf("Block cache is emtry. Create\n");
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname, const std::string& store_name)
    : 
      env_(raw_options.env),
      
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      mem_options_(raw_options),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      store_name_(store_name == "" ? dbname : store_name),
      flush_size_(options_.write_buffer_size * (0.8 + 0.4 * random_double())),
      table_cache_(raw_options.table_cache ? raw_options.table_cache : new TableCache(store_name == "" ? dbname : store_name, options_, TableCacheSize(options_)) ),
      shutting_down_(nullptr),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      background_compaction_scheduled_manual_(false),
      background_split_scheduled_(false),
      background_split_mem_to_imm_(false),
      background_merge_scheduled_(false),
      manual_compaction_(nullptr),
      query_compaction_(false),
      has_flush_wait_(false),
      has_too_many_level0_files_(false),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)),
      compaction_mode_(raw_options.compaction_mode),
      append_level_(2),
      append_max_file_num_(64)
                                {
  table_cache_->options_.comparator = &internal_comparator_;
  table_cache_->options_.filter_policy = options_.filter_policy;

  background_compaction_scheduled_level_ = std::vector<bool>(config::kNumLevels, false);
  has_imm_.Release_Store(nullptr);
  bucket_ = nullptr;
  versions_->current()->RandomizeDelay();
  mem_options_.append_mode = options_.mem_append;
  mem_options_.skiplist_insertion = options_.skiplistrep;
  // create 2 free hash table for use
  for (int i = 0; i < 2; ++i) {
    hashtable_free_.push_back(new HashTable());
  }


}

DBImpl::~DBImpl() {
  Status s;
  Log(options_.info_log,"===Close DB: %s === %s", dbname_.c_str(), versions_->current()->DebugString().c_str());
  std::string summary;
  GetProperty("leveldb.stats", &summary);
  Log(options_.info_log,"===Level Status: ===\n%s ", summary.c_str());

  // Wait for background work to finish
  mutex_.Lock();
  
  
  // wait for background compaction
  for (int i = 0; i < config::kNumLevels - 1; i++) {
    while(background_compaction_scheduled_level_[i])
      background_work_finished_signal_.Wait();
  }
  
  while (background_compaction_scheduled_ || background_compaction_scheduled_manual_ || background_split_scheduled_ ) {
    background_work_finished_signal_.Wait();
  }

  shutting_down_.Release_Store(this);  // Any non-null value is ok
  
  Log(options_.info_log, "Writers_ : %d", (int)writers_.size());
  assert(imm_ == nullptr);

  // save manifest
  WritableFile* descriptor_file = nullptr;
  std::string new_manifest_file = DescriptorFileName(dbname_, 10);
  env_->DeleteFile(new_manifest_file);
  s = env_->NewWritableFile(new_manifest_file, &descriptor_file);
  unique_ptr<WritableFileWriter> file_writer(
            new WritableFileWriter(std::move(descriptor_file), new_manifest_file, EnvOptions(), false));

  log::Writer descriptor_log(
          std::move(file_writer), 0,
          false, false);
  s = versions_->WriteSnapshot(&descriptor_log);

  if (s.ok() && !new_manifest_file.empty()) {
    s = SetCurrentFile(env_, dbname_, 10);
    descriptor_log.WriteBuffer();
  }

  mutex_.Unlock();

  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }

  delete versions_;
}

Status DBImpl::NewDB() {
  // create new db
  VersionEdit new_db;
  // set comparator
  new_db.SetComparatorName(user_comparator()->Name());
  // log number reset 0
  new_db.SetLogNumber(0);
  //set nextfile number to 2
  new_db.SetNextFile(2);
  // set sequence to 0
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1); // MANIFEST-000001
  
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  unique_ptr<WritableFileWriter> file_writer(
            new WritableFileWriter(std::move(file), manifest, EnvOptions(), false));
  if (!s.ok()) {
    return s;
  }

  {
    // use log writer to write manifest
    log::Writer log(
            std::move(file_writer), 0,
            false, false);
    std::string record;
    // save db info to manifest
    new_db.EncodeTo(&record); 
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }

  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }

  return Status();
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}


void DBImpl::DeleteObsoleteFilesKV() {
  // delete obsolete files
  // evict useless tables in table_cache 
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }
  // Make a set of all of the live sstable
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFilesKV(&live);
  
  std::set<uint64_t> deleted;
  for (auto number : evicted_) {
      bool evict = live.find(number) == live.end();

      if (evict) {
        table_cache_->Evict(number);
        deleted.insert(number);
        Log(options_.info_log, "Evict Table #%llu. Cache used: %d", (unsigned long long)number, (int)table_cache_->cache_->TotalCharge());
        auto filename = TableFileName(store_name_, number);
        Log(options_.info_log, "Delete %s\n", filename.c_str());
        
        env_->DeleteFile(filename);
      }
  }

  for (auto number : deleted) {
    evicted_.erase(number);
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest, bool force_new) {
  // read manifest, recover version

  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  
  env_->CreateDir(dbname_);

  // assert(db_lock_ == nullptr);
  // Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  // if (!s.ok()) {
  //   return s;
  // }
  Status s;

  // create a new db if not exist yet
  if (!env_->FileExists(CurrentFileName(dbname_)) || force_new) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }

  SequenceNumber max_sequence(kv_->LastSequence());

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }
  else {
    kv_->SetLastSequence(versions_->LastSequence());
  }

  if (kv_->FileNumber() < versions_->FileNumber()) {
    kv_->SetFileNumber(versions_->FileNumber());
  }
  
  Log(options_.info_log, "=== Recover info === %s \n %s", dbname_.c_str(), versions_->current()->DebugString().c_str());
  return Status::OK();
}


Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr, 0);
  return s;
}

Status DBImpl::WriteLevel0TableKV(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  // the logic gurrente that there is space in level0
  mutex_.AssertHeld();
  
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;

  meta.number = kv_->NewFileNumber(); // table number
  versions_->MarkFileNumberUsed(meta.number);

  meta.level = 0;

  pending_outputs_.insert(meta.number);
  Arena arena;
  Iterator* iter = mem->NewIterator(ReadOptions(), &arena);

  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "Minor compaction start (table #%llu.)... %s. %s", 
          (unsigned long long)meta.number,
          versions_->LevelSummary(&tmp),  
          dbname_.c_str());
  uint64_t bs = env_->NowMicros();
  Status s;
  {
    mutex_.Unlock();
    // slow disk write release lock
    // write a table at current version's level0 file, at offset: initial_offset
    s = BuildTableKV(store_name_, env_, options_, table_cache_, iter, &meta);
    bucket_->last_flush_seq.store(meta.largest.sequence(), std::memory_order_release);
    mutex_.Lock();
  }
  uint64_t be = env_->NowMicros();

  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    env_->SetFlushTrue(); // indicate a flush happens
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest, meta.median, meta.count);
  }


  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;

  stats.bytes_written = meta.file_size;

  Log(options_.info_log, "Level-0 table #%llu(Count: %llu, %llu us: %s - %s - %s): %llu KB (%.2f MB/s). %s. %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.count,
      (unsigned long long) (be - bs),
      meta.smallest.DebugString().c_str(),
      meta.median.DebugString().c_str(),
      meta.largest.DebugString().c_str(),
      (unsigned long long) meta.file_size / 1024,
      (double)stats.bytes_written / stats.micros *0.953674,
      s.ToString().c_str(), 
      dbname_.c_str());

  stats_[level].Add(stats);
  env_->stats_[level].Add(stats); // add global stats

  return s;
}
// REQUIRES: mutex_ is held


void DBImpl::CompactMemTableKV() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  
  Status s = WriteLevel0TableKV(imm_, &edit, base);

  base->Unref();
  // here we finish file write and VersionEdit update
  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }
  

  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_); 
    
    s = versions_->LogAndApplyKV(&edit, &mutex_); 
  }
  
  if (s.ok()) {
    // Commit to the new state
    hashtable_free_.push_back(imm_->GetHashTable());
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    DeleteObsoleteFilesKV();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  Log(options_.info_log, "Manual compaction.");
  ManualCompaction manual;
  manual.done = false;

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

void DBImpl::AddFileToLastLevel(FileMetaData* meta) {
  // add to splitlevel
  VersionEdit edit;
  mutex_.Lock();
  edit.AddFile(kSplitLevel, meta->number, meta->file_size, meta->smallest, meta->largest, meta->median, meta->count);
  versions_->LogAndApplyKV(&edit, &mutex_);
  Log(options_.info_log, "Bucket %s Add File %llu to Split Level. %s", dbname_.c_str(), (unsigned long long)meta->number, versions_->current()->DebugString().c_str());

  mutex_.Unlock();
}



void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok() && !s.IsTryAgain()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

std::vector<std::string> GetTPivots(int T, std::string smallest, std::string median, std::string largest) {
  std::vector<std::string> pivots(T - 1, "");
  int s_size = smallest.size(), l_size = largest.size();
  assert(s_size == l_size);

  std::vector<int> diff_left(T / 2, 0), diff_right(T / 2, 0);
  for (int i = 0; i < s_size; ++i) {
    diff_left[i] = median[i] - smallest[i];
    diff_right[i] = largest[i] - median[i];
  }


}
// Require mutex_ hold
// do background compaction or split current bucket when necessary
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();

  // schedule minor compaction 
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr) {
    // No imm and no manual work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(
                  &DBImpl::BGWorkFlush, 
                  this,                // argument that passes to BGWorkFlush, here is DBImpl*
                  Env::Priority::HIGH, // put in High thread pool
                  this,                // tag: used to identify this function is called from which bucket
                  nullptr, 
                  kBGWorkFlushTag,     // identify this compaction is minor compaction
                  false);              // put at the end of the queue
    Log(options_.info_log, "Schedule FLush in High Pool(Level0 has table %d): High Queue: %d. Low Queue: %d", (int)versions_->NumLevelFiles(0), env_->GetThreadPoolQueueLen(Env::Priority::HIGH), env_->GetThreadPoolQueueLen(Env::Priority::LOW));
  }

  // schedule major compaction 
  {
    for (int level = 0; level < kSplitLevel; level++) {
      if (background_compaction_scheduled_level_[level]) {
        // Already scheduled
        // Log(options_.info_log, "Level-%d compaction already scheduled.", level);
      } else if (shutting_down_.Acquire_Load()) {
        // DB is being deleted; no more background compactions
      } else if (!bg_error_.ok()) {
        // Already got an error; no more changes
      } else if (!versions_->NeedsCompactionAtLevel(level) && !versions_->ReadTriggerCompaction(level)) {
        // No work to be done
      } else if (versions_->NeedsCompactionAtLevel(level)){ 
        // write triggered compaction
        background_compaction_scheduled_level_[level] = true;
        auto ca = new CompactionArg;
        ca->db = this;
        ca->level = level;
        env_->Schedule(&DBImpl::BGWorkCompaction, 
                       ca, Env::Priority::LOW, 
                       this,    // tag: used to identify this function is called from which bucket
                       nullptr, 
                       level,
                       false);  // put at end of the queue
        Log(options_.info_log, "Schedule Compaction in Low Pool Level-%d: High Queue: %d. Low Queue: %d", level, env_->GetThreadPoolQueueLen(Env::Priority::HIGH),env_->GetThreadPoolQueueLen(Env::Priority::LOW));
      } else if (versions_->ReadTriggerCompaction(level) && !versions_->NeedsSplit()) {
        // if bucket needs split, then we don't trigger read compaction
        background_compaction_scheduled_level_[level] = true;
        auto ca = new CompactionArg;
        ca->db = this;
        ca->level = level;
        env_->Schedule(&DBImpl::BGWorkCompaction, 
                        ca, 
                        Env::Priority::LOW, 
                        this,   // tag: used to identify this function is called from which bucket
                        nullptr, 
                        level,  // identify which level's compaction is scheduled
                        true);  // put at front of the queue
        Log(options_.info_log, "Read Triggered Compaction in Low Pool Level-%d: High Queue: %d. Low Queue: %d", level, env_->GetThreadPoolQueueLen(Env::Priority::HIGH),env_->GetThreadPoolQueueLen(Env::Priority::LOW));
      }
    }
  }
  

  { // schedule manual compaction or query triggered compaction
    // wait for all the other compaction
    if (background_compaction_scheduled_manual_ 
      || background_compaction_scheduled_
      || background_compaction_scheduled_level_[0]
      || background_compaction_scheduled_level_[1]
      || background_compaction_scheduled_level_[2]
      || background_compaction_scheduled_level_[3]
      || background_compaction_scheduled_level_[4]
      || background_compaction_scheduled_level_[5]) {
      // Already scheduled
    } else if (shutting_down_.Acquire_Load()) {
      // DB is being deleted; no more background compactions
    } else if (!bg_error_.ok()) {
      // Already got an error; no more changes
    } else if (manual_compaction_ == nullptr && query_compaction_ == false) {
      // No work to be done
    } else {
      background_compaction_scheduled_manual_ = true;
      env_->Schedule(&DBImpl::BGWorkCompactionManual, this, Env::Priority::LOW, this);
    }
  }

  // only execute once
  if (versions_->NeedsSplit() && bucket_->spliting_status == kSplitIdle) {
    bucket_->split_pivots = versions_->GeneratePivots(bucket_->T); // generate T-1 pivot to split to T buckets
    bucket_->split_pivots.push_back(bucket_->largest);
    bucket_->spliting_status = kShouldSplit;
  }
  
  // schedule split
  // wait for all the other compaction
  if (background_split_scheduled_ 
      || background_compaction_scheduled_
      || background_compaction_scheduled_level_[0]
      || background_compaction_scheduled_level_[1]
      || background_compaction_scheduled_level_[2]
      || background_compaction_scheduled_level_[3]
      || background_compaction_scheduled_level_[4]
      || background_compaction_scheduled_level_[5]) {
    // already scheduled
  } else if (shutting_down_.Acquire_Load()) {
      // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
      // Already got an error; no more changes
  } else if (versions_->NeedsSplit()) { // should schedule split
    background_split_scheduled_ = true;
    // update buckets split_pivot
    Log(options_.info_log, "Schedule BGWorkSplit. %s", dbname_.c_str());
    env_->Schedule(&DBImpl::BGWorkSplit, this, Env::Priority::BOTTOM, this );
  }
  
  // schedule Merge
  if (background_merge_scheduled_) {

  } else if (shutting_down_.Acquire_Load()) {
      // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
      // Already got an error; no more changes
  } else if (bucket_->spliting_status == kSplitFinish){
    background_merge_scheduled_ = true;
    Log(options_.info_log, "Bucket spliting finish. Schedule Merge. %s", dbname_.c_str());
    MergeArgs* arg = new MergeArgs();
    arg->bucket = bucket_;
    arg->kv = kv_;
    env_->Schedule(&KV::BGWorkMerge, arg, Env::Priority::LOW, this, nullptr, kBGWorkMergeAfterSplit, false);
  }
}

void DBImpl::BGWorkSplit(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallSplit();
}

void DBImpl::BGWorkFlush(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallFlush();
}


void DBImpl::BackgroundCallFlush() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
      if (imm_ != nullptr) CompactMemTableKV();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();

  // wake up MakeRoomForWrite wait.
  background_work_finished_signal_.SignalAll();
}


void DBImpl::BGWorkCompaction(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  reinterpret_cast<DBImpl*>(ca.db)->BackgroundCallCompaction(ca.level);
}

void DBImpl::BackgroundCallCompaction(int level) {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_level_[level]);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction(level);
  }

  background_compaction_scheduled_level_[level] = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();

  background_work_finished_signal_.SignalAll();
}

// REQUIRES: mutex_ is held
void DBImpl::BackgroundCompaction(int level) {
  mutex_.AssertHeld();

  uint64_t start = env_->NowMicros();

  Compaction* c;

  // save file metadata to be compacted to c 
  c = versions_->PickCompactionKVAtLevel(level);
  
  Status status;
  if (c == nullptr) {
    // Nothing to do
  }
  else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWorkKV(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFilesKV();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Level-%d Major Compaction error: %s", (int) level, status.ToString().c_str());
  }

  Log(options_.info_log, "Level-%d Major Compaction end (%llu us) ...%s. %s\n", 
      level, 
      (unsigned long long)env_->NowMicros() - start, 
      dbname_.c_str(),
      versions_->current()->DebugString().c_str());
  
}

void DBImpl::BGWorkCompactionManual(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallCompactionManual();
}

void DBImpl::BackgroundCallCompactionManual() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_manual_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompactionManual();
  }

  background_compaction_scheduled_manual_ = false;
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}


void DBImpl::BackgroundCallSplit() {
  MutexLock l(&mutex_);
  assert(background_split_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundSplit();
  }
  background_split_scheduled_ = false;
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}


void DBImpl::BackgroundSplit() {
  mutex_.AssertHeld();
  uint64_t start = env_->NowMicros();

  if (imm_ != nullptr) { 
    Log(options_.info_log, "Split wait flush complete. Terminate. %s", dbname_.c_str());
    return;
  }

   
  for (int i = 0; i < config::kNumLevels; ++i) {
    if (versions_->NeedsCompactionAtLevel(i)) {
      Log(options_.info_log, "Split wait Level-%d major compaction finish. Terminate %s", i, dbname_.c_str());
      return ;
    }
  }

  assert(imm_ == nullptr);
  while(!writers_.empty()) {
    mutex_.Unlock();
    Log(options_.info_log, "Split sleep. writers_ not emptry %s", dbname_.c_str());
    env_->SleepForMicroseconds(1000);
    mutex_.Lock();
  }

  
  if (!background_split_mem_to_imm_) {
    Log(options_.info_log, "Split wait. mem to imm %s", dbname_.c_str());
    background_split_mem_to_imm_ = true;
    imm_ = mem_;
    has_imm_.Release_Store(imm_);
    // create mem for read only
    mem_ = new MemTable(internal_comparator_, hpblock_, mem_options_, flush_size_, hashtable_free_.front());
    hashtable_free_.pop_front();
    mem_->Ref();
    return;
  }

  Compaction* c = nullptr;
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "Split pick all tables. %s %s", versions_->LevelSummary(&tmp), dbname_.c_str());
  c = versions_->PickCompactionForSplit(); 
  
  Status status;
  assert(c != nullptr);

  CompactionState* compact = new CompactionState(c);
  bucket_->spliting_status = kSpliting;
  status = DoSplitWork(compact);
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  
  CleanupCompaction(compact);
  c->ReleaseInputs();
  DeleteObsoleteFilesKV();
  

  delete c;
  
  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Split Error: %s %s", status.ToString().c_str(), dbname_.c_str());
  }

  bucket_->spliting_status = kSplitFinish;
  Log(options_.info_log, "Bucket %s split end (%llu us) ...%s\n", dbname_.c_str(), (unsigned long long)env_->NowMicros() - start, versions_->current()->DebugString().c_str());
  for (int i = 0; i < bucket_->bottom_tables.size(); ++i) {
    Log(options_.info_log, "Splitted BottomTable %d (%llu). Number: %llu, File Size: %llu. [%s - %s - %s]", 
      i,
      (unsigned long long)bucket_->bottom_tables[i].count, 
      (unsigned long long)bucket_->bottom_tables[i].number, 
      (unsigned long long)bucket_->bottom_tables[i].file_size, 
      bucket_->bottom_tables[i].smallest.DebugString().c_str(),
      bucket_->bottom_tables[i].median.DebugString().c_str(),
      bucket_->bottom_tables[i].largest.DebugString().c_str());
  }
}


// in charge of manual compaction and query triggered compaction
void DBImpl::BackgroundCompactionManual() {
  mutex_.AssertHeld();
  uint64_t start = env_->NowMicros();

  Compaction* c = nullptr;
  
  bool is_manual = (manual_compaction_ != nullptr);
  bool is_query = query_compaction_;

  if (is_manual) {
    c = versions_->ManualPickCompactionKV(); 
  } else if (is_query) {
    c = versions_->PickCompactionKVForQuery();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  }
  else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWorkKV(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFilesKV();
  }
  delete c;
  
  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Manual Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    m->done = true;
    manual_compaction_ = nullptr;
    Log(options_.info_log, "Level-%d Manual Major Compaction end (%llu us) ...%s. %s\n", versions_->current()->LevelToBeCompacted(), (unsigned long long)env_->NowMicros() - start, versions_->current()->DebugString().c_str(), dbname_.c_str());
  }
  if (is_query) {
    query_compaction_ = false;
    Log(options_.info_log, "Level-%d Query Compaction end (%llu us) ...%s. %s\n", versions_->current()->LevelToBeCompacted(), (unsigned long long)env_->NowMicros() - start, versions_->current()->DebugString().c_str(), dbname_.c_str());
  }

}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}


Status DBImpl::OpenCompactionOutputFileKV(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t number;
  int target_level = compact->compaction->level() + 1;
  CompactionState::Output out;
  {
    mutex_.Lock();
    number = kv_->NewFileNumber();
    versions_->MarkFileNumberUsed(number);
    pending_outputs_.insert(number);
    out.number = number;
    out.smallest.Clear();
    out.largest.Clear();
    out.file_size = 0;
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  
  Log(options_.info_log,  "Level-%d Major compaction to table#%llu %s",
      compact->compaction->level(),
      (unsigned long long)out.number,
      dbname_.c_str());

  // Make the output file
  std::string fname = TableFileName(store_name_, number);

  EnvOptions env_options;
  env_options.use_direct_writes = options_.direct_io;
  env_options.no_close = options_.no_close;
  env_options.rate_limiter = NewGenericRateLimiter(1 << 30, 100000, 10);
  WritableFile* file;
  Status s = env_->NewWritableFile(fname, &file, env_options);
  file->SetIOPriority(Env::IO_LOW);
  WritableFileWriter* file_writer = 
            new WritableFileWriter(std::move(file), fname, env_options, true);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, file_writer);
    compact->outfile = file_writer;
  }
  else{
    Log(options_.info_log, "=== NewWritableFileAtOffset fail. %s", s.ToString().c_str() );
  }
  return s;
}


Status DBImpl::FinishCompactionOutputFileKV(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);
  FileMetaData meta;
  CompactionState::Output* out = compact->current_output();
  
  assert(out->number != 0);

  // Check for iterator errors
  Status s = input->status();
  // Log(options_.info_log, "Iterator error: %s", s.ToString().c_str());
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  // Log(options_.info_log, "Finish status: %s", s.ToString().c_str());
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync(false);
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }

  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    meta.number = out->number;
    meta.level = compact->compaction->level() + 1;
    meta.file_size = current_bytes;

    // Verify that the table is usable
    uint64_t vb = env_->NowMicros();
    Iterator* iter = table_cache_->NewIteratorKV(ReadOptions(), &meta);


    uint64_t ve = env_->NowMicros();
    s = iter->status();
    Log(options_.info_log, "=== Verify compacted table(%llu us): %s",(unsigned long long ) ve - vb ,s.ToString().c_str());
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes.",
          (unsigned long long) meta.number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes
          );
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResultsKV(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // add the old table to delete list
  compact->compaction->AddInputDeletions(compact->compaction->edit());

  // level: number
  
  const std::set< std::pair<int, uint64_t> >& deleted_files = compact->compaction->edit()->GetDeleteFiles();

  // add new tables to edit
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest, out.median, out.count);
  }

  // install new version
  Log(Env::Default()->info_log_, "Old version Delay. %s ", versions_->current()->PrintDelay().c_str());
  Status s = versions_->LogAndApplyKV(compact->compaction->edit(), &mutex_);
  versions_->current()->RandomizeDelay(compact->compaction->level()); // change delay value at compaction level
  Log(Env::Default()->info_log_, "New version Delay. %s ", versions_->current()->PrintDelay().c_str());
  // // evit the deleted table from table cache
  for (auto& df: deleted_files) {
    // table_cache_->Evict(df.second);
    evicted_.insert(df.second);
    Log(options_.info_log, "Prepare evict Level-%d@ Table #%llu. Cache used: %llu", df.first ,(unsigned long long)df.second, (unsigned long long)table_cache_->cache_->TotalCharge());
  }
  return s;
}


Status DBImpl::DoSplitWork(CompactionState* compact) {
  // make sure there is no imm table
  assert(imm_ == nullptr);

  kv_->split_work_.fetch_add(1); // used to notify kv to remain alive

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
  
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,  "Bucket %s split start ... %d files. %s",
      dbname_.c_str(),
      compact->compaction->num_input_files(0),
      versions_->LevelSummary(&tmp));

  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);

  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  // create iterator to get the pivots
  uint64_t total_count = 0;
  Iterator* input = versions_->MakeInputIteratorKV(compact->compaction, total_count);
  Iterator* input_slow = versions_->MakeInputIteratorKV(compact->compaction, total_count);
  bool should_be_median = true;

  input->SeekToFirst();
  input_slow->SeekToFirst();

  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  int count = 0;
  
  // iterate second time
  for (input->SeekToFirst(); input->Valid(); ) {
    Slice key = input->key();
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } 

      last_sequence_for_key = ikey.sequence;
    }

    if (compact->builder && should_be_median) {
      input_slow->Next();
    }
    should_be_median = !should_be_median;

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        // create a new table builder
        status = OpenCompactionOutputFileKV(compact);
        if (!status.ok()) {
          break;
        }
      }

      // Split tables when reach to split pivot
      int current_pivot_index = bucket_->bottom_tables.size();
      if (bucket_->bottom_tables.size() >= bucket_->T) {
        printf("error\n");
        exit(1);
      }
      if (user_comparator()->Compare(ikey.user_key,
                                     Slice(bucket_->split_pivots[current_pivot_index].data(), 
                                           bucket_->split_pivots[current_pivot_index].size())) > 0) {
        compact->current_output()->count = count;
        // use the median we encounter until now.
        compact->current_output()->median.DecodeFrom(input_slow->key());
        status = FinishCompactionOutputFileKV(compact, input);
        FileMetaData bottom_table;
        bottom_table.number = compact->current_output()->number;
        bottom_table.file_size = compact->current_output()->file_size;
        bottom_table.level = compact->compaction->level() + 1;
        bottom_table.count = compact->current_output()->count;
        bottom_table.smallest = compact->current_output()->smallest;
        bottom_table.median = compact->current_output()->median;
        bottom_table.largest = compact->current_output()->largest;
        bucket_->bottom_tables.push_back(bottom_table);
        // reset
        input_slow->Seek(input->key());
        count = 0;
        if (!status.ok()) {
          break;
        }
        status = OpenCompactionOutputFileKV(compact);
        if (!status.ok()) {
          break;
        }
      }
      count++; // a new key is append at sstable. will reset after a new table is generated

      // update smallest and largest metadata
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());
      uint64_t write_start = env_->NowMicros();
    }
    input->Next();
  }


  if (status.ok() && shutting_down_.Acquire_Load()) {
    // status = Status::IOError("Deleting DB during split");
    Log(options_.info_log, "Deleting DB during split ");
  }

  if (status.ok() && compact->builder != nullptr) {
    compact->current_output()->count = count;
    compact->current_output()->median.DecodeFrom(input_slow->key());
    status = FinishCompactionOutputFileKV(compact, input);
    FileMetaData bottom_table;
    bottom_table.number = compact->current_output()->number;
    bottom_table.file_size = compact->current_output()->file_size;
    bottom_table.level = compact->compaction->level() + 1;
    bottom_table.count = compact->current_output()->count;
    bottom_table.smallest = compact->current_output()->smallest;
    bottom_table.median = compact->current_output()->median;
    bottom_table.largest = compact->current_output()->largest;
    bucket_->bottom_tables.push_back(bottom_table);
  }

  if (status.ok()) {
    status = input->status();
  }

  delete input;
  delete input_slow;
  input_slow = nullptr;
  input = nullptr;
  

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;

  uint64_t install_time = env_->NowMicros();

  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);
  env_->stats_[compact->compaction->level() + 1].Add(stats); // add global stats

  if (status.ok()) {
    VersionEdit* edit = compact->compaction->edit();
    //  LogAndApply write new version
    Log(options_.info_log,  "Bucket %s split install. ...",
      dbname_.c_str());

    status = InstallCompactionResultsKV(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  

  Log(options_.info_log,
      "Level-%d Split Version install: %llu us. %s %s",compact->compaction->level(), (unsigned long long) env_->NowMicros() - install_time, versions_->LevelSummary(&tmp), dbname_.c_str());
  
  versions_->ClearSplit();
  return status;
}

int64_t DBImpl::TotalSize() {
  return versions_->current()->TotalSize();
}

Status DBImpl::DoCompactionWorkKV(CompactionState* compact) {
  // merge compaction tables in level to level+1
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
  
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,  "Level-%d Major compaction start ... %d files. %s %s",
      compact->compaction->level(),
      compact->compaction->num_input_files(0),
      versions_->LevelSummary(&tmp), dbname_.c_str());

  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);

  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  bool switch_to_low_speed = false;

  // get combined ierator using metadata in compaction 
  uint64_t total_count = 0;
  Iterator* input = versions_->MakeInputIteratorKV(compact->compaction, total_count);
  Iterator* input_slow = versions_->MakeInputIteratorKV(compact->compaction, total_count);
  bool should_be_median = true;

  input->SeekToFirst();
  input_slow->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  // iterator order: increasing key, decreasing sequence_number 
  int count = 0;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (imm_ != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr && background_compaction_scheduled_ == false ) {
        background_compaction_scheduled_ = true;
        CompactMemTableKV();
        background_compaction_scheduled_ = false;
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }
  
    Slice key = input->key();

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // ignore the key if we see it before 
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } 
      last_sequence_for_key = ikey.sequence;
    }


    if (compact->builder && should_be_median) {
      compact->current_output()->median.DecodeFrom(input_slow->key());
      input_slow->Next();
    }
    should_be_median = !should_be_median;

    if (!drop) {
      count++; // a new key is append at sstable
      // Open output file if necessary
      if (compact->builder == nullptr) {
        // create a new table builder at target level
        status = OpenCompactionOutputFileKV(compact);
        if (!status.ok()) {
          break;
        }
      }
      // update smallest and largest metadata
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

    }

    input->Next();
  }


  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
    Log(options_.info_log, "Deleting DB during compaction ");
  }


  if (status.ok() && compact->builder != nullptr) {
    compact->current_output()->count = count;
    status = FinishCompactionOutputFileKV(compact, input);
  }
  if (status.ok()) {
    status = input->status();
    
  }
  delete input;
  delete input_slow;
  input_slow = nullptr;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;


  uint64_t install_time = env_->NowMicros();

  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  Log(options_.info_log,
      "Level-%d Major compaction end (Imm: %llu us, Merge: %llu us. %.2f MB/s) %s", compact->compaction->level(), (unsigned long long)imm_micros, (unsigned long long)stats.micros, (double)stats.bytes_written/stats.micros, dbname_.c_str() );

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);
  env_->stats_[compact->compaction->level() + 1].Add(stats); // add global stats
  if (status.ok()) {
    VersionEdit* edit = compact->compaction->edit();
    status = InstallCompactionResultsKV(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  

  Log(options_.info_log,
      "Level-%d Major compaction Version install: %llu us. %s. %s",compact->compaction->level(), (unsigned long long) env_->NowMicros() - install_time, versions_->LevelSummary(&tmp), dbname_.c_str());
  
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) { }
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed, Arena* arena) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator(ReadOptions(), arena));
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator(ReadOptions(), arena));
    imm_->Ref();
  }
  versions_->current()->AddIteratorsKV(options, &list);
  if (list.size() - versions_->current()->NumFiles(kSplitLevel) >= 6) {
    query_compaction_ = true;
    MaybeScheduleCompaction();
  }

  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}


int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status  DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) {
    imm->Ref(); 
  }
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
        // auto start = Env::Default()->NowMicros();
        s = current->GetKV(options, lkey, value, &stats);
        // Log(options_.info_log, "Get Key time: %llu", (unsigned long long)(Env::Default()->NowMicros() - start));
        have_stat_update = true;
    } 
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    // if read trigger compaction ,then we reset delay
    current->ResetDelay(current->file_to_compact_level_);
    versions_->FinalizeKV(current);
    MaybeScheduleCompaction();
  }

  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}



Iterator* DBImpl::NewIterator(const ReadOptions& options, Arena* arena) {
  assert(arena != nullptr);
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed, arena);
  
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != nullptr
       ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val, uint64_t seq) {
  return DB::Put(o, key, val, seq);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key, uint64_t seq) {
  return DB::Delete(options, key, seq);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch, uint64_t seq) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  // locks the mutex, blocks if the mutex is not available 
  MutexLock l(&mutex_);

  // check the split status, if split is scheduled, the return 
  if (background_split_scheduled_) {
    return Status::Splitting();
  }

  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait(); 
  }
  if (w.done) {
    return w.status;
  }

  // if(w.sync) fprintf(stdout, "sync1\n");
  // May temporarily unlock and wait.
  // all the difference is the compaction logic 
  // This function control the comapction logic
  Status status = MakeRoomForWriteKV(my_batch == nullptr);

  uint64_t last_sequence = seq == 0 ? versions_->LastSequence() : seq;
  Writer* last_writer = &w;
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);
    // fprintf(stdout, "1 batch: %d keys\n", WriteBatchInternal::Count(updates));
    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {

      mutex_.Unlock();
      status = WriteBatchInternal::InsertInto(updates, mem_);
      mutex_.Lock();
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWriteKV(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  // bool allow_delay = !force;
  
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } 
    else if (!force &&
               (mem_->ApproximateMemoryUsage() <= (uint32_t)mem_->flush_size_)) { // ApproximateMemoryUsage() return the space arena occupy, not the actual space been used
      // There is room in current memtable
      break;
    } 
    else if (imm_ != nullptr) { // 
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.

      // set up-limit of current memtable to 2X of the original size to avoid blocking
      if (!mem_->double_flush_size_once_) {
        mem_->double_flush_size_once_ = true;
        mem_->flush_size_ = 2 * mem_->flush_size_;
        Log(options_.info_log, "Mem Table Flush Double Once. to %d kB. %s 's mem: %d", mem_->flush_size_ / 1024, dbname_.c_str(), mem_);
      }
      else 
      {
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log, "Mem Table full; waiting compaction .... %s. High queue: %d, Low queue: %d\n", versions_->LevelSummary(&tmp), env_->GetThreadPoolQueueLen(Env::Priority::HIGH), env_->GetThreadPoolQueueLen(Env::Priority::LOW)) ;
        uint64_t start = env_->NowMicros();
        has_flush_wait_ = true;
        
        if (background_compaction_scheduled_) {
          int rescheduleflush = env_->UnSchedule(this, Env::Priority::HIGH, kBGWorkFlushTag);
          if (rescheduleflush != 1) {
            Log(options_.info_log, "******** Warning: Flush job is already working. (%d) ********* ", rescheduleflush);
          }
          else { // flush not work yet, reschedule to top
            env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::HIGH, this, nullptr, kBGWorkFlushTag, true); // 将 flush 安排到队列首
            Log(options_.info_log, "=== Priority Flush compaction. High Queue: %d", env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
          }
        }
        env_->FlushWaitAdd();
        background_work_finished_signal_.Wait(); // 等待 background 的 thread 完成 flush，并 notify
        env_->FlushWaitDec();
        Log(options_.info_log, "Imm Table full; waiting released(%llu us). ... %s\n", (unsigned long long) env_->NowMicros() - start, versions_->LevelSummary(&tmp));
        has_flush_wait_ = false;
      }
    } 
    else if (versions_->NumLevelFiles(0) >= 16) { 
      // There are too many level-0 files.
      VersionSet::LevelSummaryStorage tmp;
      Log(options_.info_log, "Too many L0 files; waiting start... High queue: %d, Low queue: %d. %s\n", env_->GetThreadPoolQueueLen(Env::Priority::HIGH), env_->GetThreadPoolQueueLen(Env::Priority::LOW), versions_->LevelSummary(&tmp)) ;
      // prioritize level 0 major compaction
      uint64_t start = env_->NowMicros();
      has_too_many_level0_files_ = true;
      if (background_compaction_scheduled_level_[0]) {
        int reschedule = env_->UnSchedule(this, Env::Priority::LOW, 0); // 移除在队列中的 level 0 compaction. 
        // assert(reschedule == 1);
        if (reschedule != 1) {
          Log(options_.info_log, "******** Warning: Level-0 compaction job is already working. (%d) ********* ", reschedule);
        }
        if (reschedule == 1) { // job not work yet, reschedule to top
          auto ca = new CompactionArg;
          ca->db = this;
          ca->level = 0;
          Log(options_.info_log, "=== Priority Level-0 compaction. Too many files. HIGH Queue before: %d ", env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
          env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::HIGH, this, nullptr, 0, true); 
          Log(options_.info_log, "=== Priority Level-0 compaction. Too many files. HIGH Queue after: %d ", env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
        }
      }
      env_->FlushWaitAdd();
      background_work_finished_signal_.Wait();
      env_->FlushWaitDec();
      Log(options_.info_log, "Too many L0 files; waiting finish: %llu ... High queue: %d, Low queue: %d. %s\n", (unsigned long long) env_->NowMicros() - start,  env_->GetThreadPoolQueueLen(Env::Priority::HIGH), env_->GetThreadPoolQueueLen(Env::Priority::LOW), versions_->LevelSummary(&tmp)) ;
      has_too_many_level0_files_ = false;
    }  
    else {
      imm_ = mem_;
      if (force) {
        Log(options_.info_log, "Force make room. mem to imm. mem used space %llu", (unsigned long long)mem_->ActualMemoryUsage());
      }
      has_imm_.Release_Store(imm_);
      assert(!hashtable_free_.empty());
      mem_ = new MemTable(internal_comparator_, hpblock_, mem_options_, flush_size_, hashtable_free_.front());
      hashtable_free_.pop_front();
      mem_->Ref();
      
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level Tables Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.2f %8.2f %9.2f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value, uint64_t seq) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch, seq);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key, uint64_t seq) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch, seq);
}

DB::~DB() { }


Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr, const std::string& store_name, void* args, KV* kv, bool force_new) {
  // store_name is where the database is stored
  *dbptr = nullptr;
  
  DBImpl* impl = new DBImpl(options, dbname, store_name);
  impl->mutex_.Lock();
  VersionEdit edit;
  if (args != nullptr) {
    impl->bucket_ = reinterpret_cast<Bucket*>(args);
    impl->kv_ = kv;
    impl->hpblock_ = impl->bucket_->hugepage;
  }
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false; 

  Status s = impl->Recover(&edit, &save_manifest, force_new);
  if (s.ok() && impl->mem_ == nullptr) {

    
    if (s.ok()) {
      assert(!impl->hashtable_free_.empty());
      impl->mem_ = new MemTable(impl->internal_comparator_, impl->hpblock_, impl->mem_options_, impl->flush_size_, impl->hashtable_free_.front());
      impl->hashtable_free_.pop_front();
      impl->mem_->Ref();
    }
  }


  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApplyKV(&edit, &impl->mutex_);
  }

  // if (s.ok()) {
  //   // impl->DeleteObsoleteFilesKV();
  //   impl->MaybeScheduleCompaction();
  // }
  
  impl->mutex_.Unlock();


  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }

  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  // fprintf(stdout, "destroy: %s\n", dbname.c_str());
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      // fprintf(stdout, "== Delete file: %s\n", filenames[i].c_str());
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
