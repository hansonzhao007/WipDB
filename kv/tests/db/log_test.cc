#include "kv/kv.h"

#include <stdint.h>
#include <stdio.h>
#include <cerrno>
#include <sys/stat.h>
#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include <cmath>

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
#include "gflags/gflags.h"
#include "util/perfsvg.h"
#include "util/file_reader_writer.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

using namespace kv;

// Number of key/values to place in database
DEFINE_int64(num, 1000000, "Number of key/values to place in database");

DEFINE_int64(value_size, 100, "");

DEFINE_int64(log_buffer_size, 65536, "");
int main(int argc, char** argv) {
    debug_perf_ppid();
    ParseCommandLineFlags(&argc, &argv, true);
    WritableFile* logfile_;
    log::Writer* log_;
    Env* env = Env::Default();
    EnvOptions env_options = env->OptimizeForLogWrite(EnvOptions());
    env_options.fallocate_with_keep_size = false;
    env_options.writable_file_max_buffer_size = FLAGS_log_buffer_size;
    env_options.use_direct_writes = true;
    std::string logname = LogFileName("/tmp", 0);
    Status status = env->NewWritableFile(logname, &logfile_, env_options);
    unique_ptr<WritableFileWriter> file_writer(
            new WritableFileWriter(std::move(logfile_), logname, env_options, true, 16 << 10));
    log_ = new log::Writer(
            std::move(file_writer), 0,
            false, true);

    uint64_t byte_written = 0;
    uint64_t start = env->NowMicros();
    std::string record(FLAGS_value_size, 'a');
    debug_perf_switch();
    for (int i = 0; i < FLAGS_num; i++) {
        log_->AddRecord(record);
        // byte_written += FLAGS_value_size;
    }
    logfile_->Flush();
    debug_perf_stop();
    uint64_t duration = env->NowMicros() - start;
    
    fprintf(stdout, "\n======= Speed %f Kops/s (%d). %f MB/s\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, FLAGS_num * FLAGS_value_size * 0.95367 / duration);
    return 0;
}