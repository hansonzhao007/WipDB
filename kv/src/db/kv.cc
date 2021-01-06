#include "kv/kv.h"

#include <stdint.h>
#include <cstdlib>
#include <stdio.h>
#include <cerrno>
#include <sys/stat.h>
#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include <cmath>
#include <fstream>
#include <iostream>

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
#include "util/string_util.h"
#include "util/mutexlock.h"
#include "db/kv_iter.h"
#include "util/file_reader_writer.h"
// #define KVLOGG

namespace kv {

uint64_t kWALMax = 128 << 20;


Status KV::Open(Options& options,
                    const std::string& name,
                    vector<std::string>& pivots, KV** dbptr, bool is_huge)
                    {
    Status s;
    *dbptr = nullptr;
    KV* kv = new KV(options, name);
    kv->is_huge_ = is_huge;
    kv->options_.table_cache = new TableCache(name, kv->options_, options.max_open_files - 50); 

    std::vector<std::string> p;
    bool res = kv->RestorePivots(name, p);

    if (!res) {
        printf("using provided pivots\n");
        p = pivots;
    }
    else {
        printf("using restored pivots\n");
    }
    // build hugepage 
    
    uint64_t write_buffer_size = options.write_buffer_size;
    uint64_t partition = p.size();

    // kWALMax = write_buffer_size * partition;
    
    s = kv->BuildBuckets(p);
    if (!s.ok()) {
        return s;
    }
    // after all bucket restore, we recover from log
    std::vector<std::string> filenames;
    s = Env::Default()->GetChildren(kv->logpath_, &filenames);
    if (!s.ok()) {
      return s;
    }
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    uint64_t log_max = kv->NextLogNumber();
    uint64_t start = Env::Default()->NowMicros();
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        if (type == kLogFile && number < log_max && number >= kv->log_number_min_) {
            logs.push_back(number);
            printf("Recover log# %llu\n", (unsigned long long)number);
        }     
      }
    }
    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    double file_size = 0;
    for (size_t i = 0; i < logs.size(); i++) {
        s = kv->RecoverLogFile(logs[i]);
        // we can delete old logs, because recoverLogFile will create new log for records
        uint64_t size;
        std::string lfname = LogFileName(kv->logpath_, logs[i]);
        Env::Default()->GetFileSize(lfname, &size);
        Env::Default()->DeleteFile(lfname);
        file_size += size;
    }
    uint64_t end = Env::Default()->NowMicros();
    printf("Log_Recover_Time, %.4f, BucketCount, %d , Log_size, %.1f ,MB\n", (end - start) / 1000000.0, (int)kv->versions_->current()->buckets_.size(), file_size /1024.0/1024.0);
    kv->PrintPivots();
    *dbptr = kv;
    return s;
}


Status KV::RecoverLogFile(uint64_t log_number) {
    struct LogReporter : public log::Reader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;  // null if options_.paranoid_checks==false
        virtual void Corruption(size_t bytes, const Status& s) {
        Log(info_log, "%s%s: dropping %d bytes; %s",
            (this->status == nullptr ? "(ignoring error) " : ""),
            fname, static_cast<int>(bytes), s.ToString().c_str());
        if (this->status != nullptr && this->status->ok()) *this->status = s;
        }
    };

    // Open the log file
    std::string fname = LogFileName(logpath_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        return status;
    }
    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : nullptr);
    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(file, &reporter, true/*checksum*/,
                        0/*initial_offset*/);
    Log(options_.info_log, "Recovering log #%llu",
        (unsigned long long) log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    while (reader.ReadRecord(&record, &scratch) &&
            status.ok()) {
        if (record.size() < 12) {
        reporter.Corruption(
            record.size(), Status::Corruption("log record too small"));
        continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        status = Write(WriteOptions(), &batch);
    }

    delete file;
    return status;
}

bool KV::RestorePivots(std::string path, vector<std::string>& res) {
    std::ifstream infile; 
   
    infile.open(path + "/" + "pivot.dat"); 
    int len;
    char data[100];
    bool success = false;
    if (infile.is_open()) {
        success = true;
        infile >> len;
        if (len > 0) {
            res.resize(len);
            printf("Restore %d pivots\n", len);
        }
        for (int i = 0; i < len; ++i) {
            infile >> res[i];
            // printf("pivot: %s\n", res[i].c_str());
        }
        uint64_t num;
        infile >> num; // recover last logfile_number
        SetLogNumber(num);
        infile >> num; 
        SetFileNumber(num); // recover file number
        infile >> num; 
        SetLastSequence(num); // recover sequence
        infile >> log_number_min_;
    }
    printf("Recover seq# %llu, log# %llu, min log#%llu, file# %llu\n", 
            (unsigned long long)LastSequence(), 
            (unsigned long long)LogNumber(), 
            (unsigned long long)log_number_min_, 
            (unsigned long long)FileNumber());
    infile.close();
    return success;
}


void KV::SavePivots(std::string path) {
    std::vector<std::string> res;
    std::ofstream outfile;
    outfile.open(path + "/" + "pivot.dat"); 
    int len;
    char data[100];
    auto& buckets = versions_->current()->buckets_;
    outfile << buckets.size() << std::endl;
    for (int i = 0; i < buckets.size(); ++i) {
        outfile << buckets[i]->largest << std::endl;
    }
    outfile << LogNumber() <<std::endl; // save last logfile_number
    outfile <<  FileNumber() <<std::endl; // save file number
    outfile << LastSequence() <<std::endl; // save sequence number
    outfile << log_number_min_ << std::endl; // save the min log number that is valid
    outfile.close();
}

KVIter* KV::NewIterator(const ReadOptions& options) {
    return NewKVIterator(this);
}

Status KV::BuildBuckets(vector<std::string>& pivots) {
    // create root folder
    Status s;
    // ignore the File exists IO error
    ::mkdir(dbname_.c_str(), 0755);
    VersionKVEdit edit;
    // number the ordered buckets
    int num = 0;
    for(uint32_t i = 0; i < pivots.size(); ++i) {
        Bucket* node = new Bucket();
        node->Ref();
        if (is_huge_) {
            node->hugepage = new HugePageBlock(options_.write_buffer_size * 6);
        }
        std::string bucket_name = dbname_ + "/" + pivots[i];
        node->options = options_;
        node->db_name = bucket_name;
        s = DB::Open(options_, bucket_name , &node->db, dbname_, node, this);
        if(!s.ok()) {
            return s;
        }
        node->largest = pivots[i];
        node->db_name = bucket_name;
        edit.AddBucket(node);
        num++;
    }

    versions_->Apply(&edit);
    return s;
}

void KV::PrintPivots() {
    std::string res = versions_->current()->BucketsInfo();
    Log(options_.info_log, "=== Print Pivots - Versions(%d) === %s", versions_->VersionCount(), res.c_str());
}

Status KV::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}

Status KV::Delete(const WriteOptions& opt, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
}

Status KV::Get(const ReadOptions& read_option, const Slice& key, std::string* value) {
    MutexLock l(&version_lock_);
    VersionKV* v = versions_->current();
    v->Ref();
    int ni = Bucket::lower_bound(v->buckets_, key);
    Bucket* n = v->buckets_[ni];
    Bucket* n_old = n->bucket_old;
    version_lock_.Unlock();

    Status s = n->db->Get(read_option, key, value);

    if (!s.ok() && n_old != nullptr) {
        s = n_old->db->Get(read_option, key, value);
    }

    version_lock_.Lock();
    v->Unref();
    
    return s;
}


void KV::BGWorkMerge(void* arg) {
  MergeArgs ca = *(reinterpret_cast<MergeArgs*>(arg));
  delete reinterpret_cast<MergeArgs*>(arg);
  reinterpret_cast<KV*>(ca.kv)->Merge(ca.bucket);
}

// merge bucekts's bottom tables to new splitted buckets
void KV::Merge(Bucket* bucket /* the bucket that is splitted*/) {
    Log(options_.info_log, "=== KV Merge %s ===. Low Queue: %d",bucket->largest.c_str(), env_->GetThreadPoolQueueLen(Env::Priority::LOW));
    assert(bucket->spliting_status == kSplitFinish);

    VersionKVEdit edit;
    MutexLock l(&version_lock_);
    VersionKV* v = versions_->current();
    v->Ref();
    // After this, split_bucket will point to the shared bucket_old 
    Bucket* split_bucket = v->FindOldBucket(bucket);
    if (split_bucket != bucket) {
        // it means bucket is not splited to T buckets, so we split it 
        Log(options_.info_log, "=== bucket %s was not splited. Split it === ", bucket->largest.c_str());
        split_bucket = *std::lower_bound(v->buckets_.begin(), v->buckets_.end(), bucket, BucketCmp());
        Log(options_.info_log, "=== Find split bucket %s. Split it === ", split_bucket->largest.c_str());
        Status s = SplitBucket(split_bucket);
        Log(options_.info_log, "Split in Merge status: %s", s.ToString().c_str());
    }

    Log(options_.info_log, "=== shared bucket refs: %d . Versions count: %d === ", split_bucket->refs_.load(), (int)versions_->VersionCount());
    
    // must have T Bottom Tables
    assert(split_bucket->bottom_tables.size()  == split_bucket->split_buckets.size());

    for (int i = 0; i < split_bucket->bottom_tables.size(); ++i) {
        Log(options_.info_log, "=== Splitted Bucket %d %s ref: %d ===", 
            i,
            split_bucket->split_buckets[i]->largest.c_str(),
            (int)split_bucket->split_buckets[i]->refs_);

        // add bottom table
        split_bucket->split_buckets[i]->db->AddFileToLastLevel(&split_bucket->bottom_tables[i]);

        // delete the old bucket link
        split_bucket->split_buckets[i]->bucket_old = nullptr;
        split_bucket->split_buckets[i]->Unref();
        split_bucket->Unref();
    }

    split_work_.fetch_add(-1);
    v->Unref();

}

void KV::GetBuckets(std::string* value) {
    value->clear();
    char buf[200];
    snprintf(buf, sizeof(buf), "BucketName, Size\n");
    value->append(buf);
    for (auto& b : versions_->current()->buckets_) {
        snprintf(buf, sizeof(buf), "%s, %lld\n", b->db_name.c_str(), (long long)b->db->TotalSize());
        value->append(buf);
    }
}
void KV::PrintBuckets() {
    std::string value;
    GetBuckets(&value);
    fprintf(stdout, "\n%s\n", value.c_str());
    fflush(stdout);
}
bool KV::GetProperty(const Slice& property, std::string* value) {
    value->clear();
    Slice in = property;
    Slice prefix("kv.");
    if (!in.starts_with(prefix)) return false;
    in.remove_prefix(prefix.size());
    if (in == "stats") {
        char buf[200];
        snprintf(buf, sizeof(buf),
                "                               Compactions\n"
                "Level Time(sec) Read(MB) Write(MB)\n"
                "--------------------------------------------------\n"
                );
        value->append(buf);
        double total_io = 0;
        double user_io = 0; 
        for (int level = 0; level < config::kNumLevels; level++) {
            if (env_->stats_[level].micros > 0) {
                snprintf(
                    buf, sizeof(buf),
                    "%3d %9.2f %8.2f %9.2f\n",
                    level,
                    env_->stats_[level].micros / 1e6,
                    env_->stats_[level].bytes_read / 1048576.0,
                    env_->stats_[level].bytes_written / 1048576.0);
                value->append(buf);
            }
            if (level == 0) user_io = env_->stats_[level].bytes_written / 1048576.0;
            total_io += env_->stats_[level].bytes_written / 1048576.0;
        }

        snprintf(buf, sizeof(buf), "BucketCount: %d\nWriteAmplification: %2.4f\n", (int)GetBuckets().size(), total_io / user_io);
        value->append(buf);

        // snprintf(buf, sizeof(buf), "BucketName, Size\n");
        // value->append(buf);
        
        // for (auto& b : versions_->current()->buckets_) {
        //     snprintf(buf, sizeof(buf), "%s, %lld\n", b->db_name.c_str(), (long long)b->db->TotalSize());
        //     value->append(buf);
        // }

        
        int64_t total_size = 0;
        versions_->current()->Ref();
        Bucket* pre_bucket = nullptr;
        for (auto& b : versions_->current()->buckets_) {
            total_size += b->db->TotalSize();
            if ((!pre_bucket && b->bucket_old != nullptr) || 
                (pre_bucket && 
                 b->bucket_old != nullptr &&
                 pre_bucket->bucket_old != b->bucket_old )) {
                 total_size += b->bucket_old->db->TotalSize();
            }
            pre_bucket = b;
        }
        versions_->current()->Unref();
        snprintf(buf, sizeof(buf), "TotalSize(MB): %.2f\n", total_size / 1024.0 / 1024.0);
        value->append(buf);
        return true;
    } 
}

void KV::PrintStats(const char* key) {
    std::string stats;
    if (!GetProperty(key, &stats)) {
        stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());

    // for (auto& b : versions_->current()->buckets_) {
    //     std::string stats;
    //     fprintf(stdout, "==== Bucket %s ====", b->largest.c_str());
    //     if (!b->db->GetProperty("leveldb.stats", &stats)) {
    //         stats = "(failed)";
    //     }
    //     fprintf(stdout, "\n%s\n", stats.c_str());
    // }
}

// split bucket n to T new buckets
Status KV::SplitBucket(Bucket* n) {
    version_lock_.AssertHeld();
    Status s;
    // bucket db is spliting and we have not create new buckets
    // if bucket n has been splited, then bypass

    if (n->spliting_status != kSplitIdle && !n->splited.load()) { 
        Log(options_.info_log, "=== SplitBucket ===");
        VersionKVEdit edit;

        // According to the T-1 pivots, create first T-1 buckets
        for (int i = 0; i < n->split_pivots.size() - 1; i++) {
            Bucket* A = new Bucket();
            if (is_huge_) {
                A->hugepage = new HugePageBlock(options_.write_buffer_size * 6);
            }
            A->Ref();
            std::string bucket_nameA = dbname_ + "/" + n->split_pivots[i];
            A->options = options_;
            A->db_name = bucket_nameA;
            A->largest = n->split_pivots[i];
            s = DB::Open(options_, bucket_nameA , &A->db, dbname_, A, this);
            if(!s.ok()) {
                Log(Env::Default()->info_log_, "Bucket %s Split open fail. %s. Bucket %s", n->db_name.c_str(), s.ToString().c_str(), n->split_pivots[i].c_str());
                return s;
            }
            // add new bucket to list
            n->split_buckets.push_back(A);
            A->Ref();
            A->bucket_old = n;
            A->bucket_old->Ref();
            edit.AddBucket(A);
        }
        
        // Create the last Bucket. Special case, becuase it share the same name with old bucket
        Bucket* B = new Bucket();
        B->Ref();
        if (is_huge_) {
            B->hugepage = new HugePageBlock(options_.write_buffer_size * 6);
        }
        std::string bucket_nameB = dbname_ + "/" + n->largest;
        B->options = options_;
        B->db_name = bucket_nameB;
        B->largest = n->largest;
        s = DB::Open(options_, bucket_nameB , &B->db, dbname_, B, this, true /* force to create a new bucket*/);
        if(!s.ok()) {
            Log(Env::Default()->info_log_, "Bucket %s Split open fail. %s. Bucket %s", n->db_name.c_str(), s.ToString().c_str(), n->largest.c_str());
            return s;
        }
        n->split_buckets.push_back(B);
        B->Ref();
        B->bucket_old = n;
        B->bucket_old ->Ref();
        edit.AddBucket(B);

        edit.DelBucket(n);
        n->splited.store(true);
        versions_->Apply(&edit);
        Log(options_.info_log, "=== SplitBucket Pivots === %s\n", versions_->current()->BucketsInfo().c_str());
    }
    else {
        Log(options_.info_log, "=== KV Split (not execute)===");
    }

    return s;
}


class KVInserter : public WriteBatch::Handler {
 public:

  const WriteOptions* write_options_;
  uint64_t sequence_;
  KV* kv_;

  KVInserter(uint64_t seq, KV* kv): sequence_(seq), kv_(kv){};

  Bucket* MaybeReSplitAndReFind(Bucket* n, const Slice& key) {
    kv_->version_lock_.AssertHeld();
    if (n->spliting_status != kSplitIdle && !n->splited.load()) {
        kv_->SplitBucket(n);
        VersionKV* v = kv_->versions_->current();
        v->Ref();
        int ni = Bucket::lower_bound(v->buckets_, key);
        n = v->buckets_[ni];
        v->Unref();
    }
    return n;
  }
  virtual void Put(const Slice& key, const Slice& value) {
    MutexLock l(&kv_->version_lock_);
    VersionKV* v = kv_->versions_->current();
    v->Ref();
    int ni = Bucket::lower_bound(v->buckets_, key);
    Bucket* n = v->buckets_[ni];
    n = MaybeReSplitAndReFind(n, key);
    n->Ref();
    kv_->version_lock_.Unlock();
    Status s = n->db->Put(*write_options_, key, value, sequence_);
    kv_->version_lock_.Lock();
    sequence_++;
    n->Unref();
    v->Unref();
  }
  
  virtual void Delete(const Slice& key) {
    MutexLock l(&kv_->version_lock_);
    VersionKV* v = kv_->versions_->current();
    v->Ref();
    int ni = Bucket::lower_bound(v->buckets_, key);
    Bucket* n = v->buckets_[ni];
    n = MaybeReSplitAndReFind(n, key);
    n->Ref();
    kv_->version_lock_.Unlock();
    Status s = n->db->Delete(*write_options_, key, sequence_);
    kv_->version_lock_.Lock();
    n->Unref();
    v->Unref();
    sequence_++;
  }
};

void KV::CompactRange(int bucket_no) {
    version_lock_.Lock();
    VersionKV* v = versions_->current();
    v->Ref();
    version_lock_.Unlock();
    if (bucket_no == -1) {
        for (auto b : v->buckets_) {
            b->db->CompactRange(nullptr, nullptr);
        }
    }
    else if (bucket_no < (int)v->buckets_.size()) {
        v->buckets_[bucket_no]->db->CompactRange(nullptr, nullptr);
    }
    version_lock_.Lock();
    v->Unref();
    version_lock_.Unlock();
}

Status KV::WriteToWAL(const WriteThread::WriteGroup& write_group, WriteBatch*& merged_batch) {
    Status status;
    if ((options_.wal_log && env_->HasFlush() && logfile_->GetFileSize() >= kWALMax ) || 
        (logfile_ == nullptr)) {
        // switch WAL
        WritableFile* lfile = nullptr;
        uint64_t old_log_number = LogNumber();
        uint64_t new_log_number = NextLogNumber();

        std::string log_fname = LogFileName(logpath_, new_log_number);
        status = env_->NewWritableFile(log_fname , &lfile, env_options_);
        lfile->SetIOPriority(Env::IO_LOW);

        unique_ptr<WritableFileWriter> file_writer(
            new WritableFileWriter(std::move(lfile), log_fname, env_options_, false));
        if(log_) delete log_;
        if(logfile_) delete logfile_;
        logfile_ = lfile;
        logfile_number_ = new_log_number;
        log_ = new log::Writer(
            std::move(file_writer), new_log_number,
            false, false);
        env_->SetFlushFalse(); // clear has_flush flag
        old_logs_.push_back(LOG(old_log_number, LastSequence()));
        DeleteObsoleteLogs();
    }
    
    merged_batch = WriteBatchInternal::MergeBatch(write_group, &tmp_batch_); // merge batch if necessary  
    if (options_.wal_log)
        status = log_->AddRecord(WriteBatchInternal::Contents(merged_batch));


    return status;
}

void KV::DeleteObsoleteLogs() {
    MutexLock l(&version_lock_);
    std::vector<std::string> filenames;
    VersionKV* v = versions_->current();
    v->Ref();
    auto buckets = v->buckets_;
    uint64_t min_seq = buckets.size() == 0 ? 0 : buckets[0]->last_flush_seq.load(std::memory_order_acquire);
    for (auto& bucket: buckets) {
        min_seq = std::min(min_seq, bucket->last_flush_seq.load(std::memory_order_acquire));
    }

    // delete log when more than 10 log files (TODO: flush when log too many)
    while(old_logs_.size() && old_logs_[0].biggest_sequence <= min_seq || old_logs_.size() > 10) {
        std::string lfname = LogFileName(logpath_, old_logs_[0].number);
        Log(options_.info_log, "Delete kv log: %s", lfname.c_str());
        env_->DeleteFile(lfname);
        old_logs_.pop_front();
        log_number_min_ = old_logs_[0].number;
    }
    v->Unref();
}


Status KV::Write(const WriteOptions& options, WriteBatch* my_batch) {
    // create a write job, prepare to put it in queue
    WriteThread::Writer w(options, my_batch);
    write_thread_.JoinBatchGroup(&w); // append to write queue, wait until become the leader or complete

    Status status;

    if (w.state == WriteThread::STATE_COMPLETED) {
        return w.FinalStatus();
    }

    // else we are the leader of the write batch group
    assert(w.state == WriteThread::STATE_GROUP_LEADER);

    // Once reaches this point, the current writer "w" will try to do its write
    // job.  It may also pick up some of the remaining writers in the "writers_"
    // when it finds suitable, and finish them in the same write batch.
    // This is how a write job could be done by the other writer.
    WriteThread::WriteGroup write_group;
    write_thread_.EnterAsBatchGroupLeader(&w, &write_group); // build batch group
    
    WriteBatch* merged_batch = nullptr;
    // only one thread will enter here
    status = WriteToWAL(write_group, merged_batch);
    
    uint64_t last_seq = LastSequence();
    KVInserter inserter(last_seq, this);
    

    inserter.write_options_ = &options;
    status = merged_batch->Iterate(&inserter);

    // clear tmp_batch_
    if (merged_batch == &tmp_batch_) {
        tmp_batch_.Clear();
    }    

    last_seq += WriteBatchInternal::Count(merged_batch);
    SetLastSequence(last_seq);
    write_thread_.ExitAsBatchGroupLeader(write_group, status); // 改变 batchgroup 里面的 writer status 为 complete

    return status;
}

KV::KV(const Options& raw_options, const std::string dbname):
    env_(raw_options.env),
    env_options_(EnvOptions()),
    options_(raw_options),
    dbname_(dbname),
    logpath_(raw_options.logpath == "" ? dbname : raw_options.logpath),
    is_huge_(false),
    logfile_(nullptr),
    logfile_number_(0),
    log_(nullptr),
    last_sequence_(0), 
    versions_(new VersionSetKV(dbname_, &options_)),
    next_file_number_(1),
    log_number_min_(0),
    split_work_(0) {
    ::mkdir(dbname_.c_str(), 0755);
    // WritableFile* lfile = nullptr;
    
    env_options_.fallocate_with_keep_size = false;
    env_options_.writable_file_max_buffer_size = raw_options.log_buffer_size; // default log buffer is 4KB
    env_options_.use_direct_writes = raw_options.log_dio; // use direct io for log
    env_options_.use_mmap_writes = false; // use mmap writes
    // env_options_.rate_limiter = NewGenericRateLimiter(80 * (1 << 20), 100000, 10); // limit log to 10MB/s
    if (options_.info_log == nullptr) {
    // Open a log file in the same directory as the db
    env_->CreateDir(dbname);  // In case it does not exist
    env_->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = env_->NewLogger(InfoLogFileName(dbname), &options_.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      options_.info_log = nullptr;
    }
    env_->info_log_ = options_.info_log;
  }
}
KV::~KV() {
    Log(options_.info_log, "========= Close KV =========.");
    printf("seq# %llu, log# %llu, min log#%llu, file# %llu\n", 
            (unsigned long long)LastSequence(), 
            (unsigned long long)LogNumber(), 
            (unsigned long long)log_number_min_, 
            (unsigned long long)FileNumber());
    
    
    if(log_) delete log_;
    if(logfile_) delete logfile_;

    // wait all the background jobs finish
    while(env_->GetThreadPoolQueueLen(kv::Env::HIGH) > 0    || 
          env_->GetThreadPoolQueueLen(kv::Env::LOW) > 0     || 
          env_->GetThreadPoolQueueLen(kv::Env::BOTTOM) > 0  ||
          split_work_.load() > 0) {
        env_->SleepForMicroseconds(1000);
    }
            
    // 1. save all the pivots
    SavePivots(dbname_);
    PrintPivots();

    // 2. unref all the buckets
    for(auto& bucket : versions_->current()->buckets_) {
        bucket->Unref();
    }

    delete versions_;

}

Status DestroyKV(const std::string& dbname, const Options& options) {
    Env* env = options.env;
    std::vector<std::string> filenames;
    Status result;
    std::string cmd =  "rm -rf " + dbname;
    int ret = system(cmd.c_str());
    
    // for(auto& b: filenames) {
    //     uint64_t number;
    //     FileType type;
    //     if (ParseFileName(b, &number, &type) &&
    //       type != kDBLockFile) {  // Lock file will be deleted at end
    //     Status del = env->DeleteFile(dbname + "/" + b);
    //     if (result.ok() && !del.ok()) {
    //       result = del;
    //     }
    //   }
    // }
    return result;
}


}
