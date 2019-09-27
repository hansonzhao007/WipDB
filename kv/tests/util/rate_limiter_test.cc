//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/rate_limiter.h"

#include <inttypes.h>
#include <chrono>
#include <limits>

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
#include "gtest/gtest.h"


using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;


// Number of key/values to place in database
DEFINE_int64(num, 10000, "Number of key/values to place in database");

DEFINE_int64(value_size, 100, "");

namespace kv {

// TODO(yhchiang): the rate will not be accurate when we run test in parallel.
class RateLimiterTest : public testing::Test {};

// #if !(defined(TRAVIS) && defined(OS_MACOSX))
// TEST_F(RateLimiterTest, Rate) {
//   auto* env = Env::Default();
//   struct Arg {
//     Arg(int32_t _target_rate, int _burst)
//         : limiter(NewGenericRateLimiter(_target_rate, 100 * 1000, 10)),
//           request_size(_target_rate / 10),
//           burst(_burst) {}
//     std::unique_ptr<RateLimiter> limiter;
//     int32_t request_size;
//     int burst;
//   };

//   auto writer = [](void* p) {
//     auto* thread_env = Env::Default();
//     auto* arg = static_cast<Arg*>(p);
//     // Test for 2 seconds
//     auto until = thread_env->NowMicros() + 2 * 1000000;
//     Random r((uint32_t)(thread_env->NowNanos() %
//                         std::numeric_limits<uint32_t>::max()));
//     while (thread_env->NowMicros() < until) {
//       for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst) + 1); ++i) {
//         arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
//                               Env::IO_HIGH, nullptr /* stats */,
//                               RateLimiter::OpType::kWrite);
//       }
//       arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1, Env::IO_LOW,
//                             nullptr /* stats */, RateLimiter::OpType::kWrite);
//     }
//   };

//   for (int i = 1; i <= 16; i *= 2) {
//     int32_t target = i * 1024 * 10;
//     Arg arg(target, i / 4 + 1);
//     int64_t old_total_bytes_through = 0;
//     for (int iter = 1; iter <= 2; ++iter) {
//       // second iteration changes the target dynamically
//       if (iter == 2) {
//         target *= 2;
//         arg.limiter->SetBytesPerSecond(target);
//       }
//       auto start = env->NowMicros();
//       for (int t = 0; t < i; ++t) {
//         env->StartThread(writer, &arg);
//       }
//       env->WaitForJoin();

//       auto elapsed = env->NowMicros() - start;
//       double rate =
//           (arg.limiter->GetTotalBytesThrough() - old_total_bytes_through) *
//           1000000.0 / elapsed;
//       old_total_bytes_through = arg.limiter->GetTotalBytesThrough();
//       fprintf(stderr,
//               "request size [1 - %" PRIi32 "], limit %" PRIi32
//               " KB/sec, actual rate: %lf KB/sec, elapsed %.2lf seconds\n",
//               arg.request_size - 1, target / 1024, rate / 1024,
//               elapsed / 1000000.0);

//       ASSERT_GE(rate / target, 0.80);
//       ASSERT_LE(rate / target, 1.25);
//     }
//   }
// }
// #endif

TEST_F(RateLimiterTest, FileWriter) {
    WritableFile* logfile_;
    log::Writer* log_;
    Env* env = Env::Default();
    EnvOptions env_options = env->OptimizeForLogWrite(EnvOptions());
    env_options.fallocate_with_keep_size = false;
    env_options.writable_file_max_buffer_size = 1 << 20;
    env_options.use_direct_writes = false;
    env_options.rate_limiter = NewGenericRateLimiter(128 * 1024, 100 * 1000, 10);
    std::string logname = LogFileName("/tmp", 0);
    Status status = env->NewWritableFile(logname, &logfile_, env_options);
    logfile_->SetIOPriority(Env::IO_LOW);
    unique_ptr<WritableFileWriter> file_writer(
            new WritableFileWriter(std::move(logfile_), logname, env_options, true, 16 << 10));
    log_ = new log::Writer(
            std::move(file_writer), 0,
            false, false);
    std::string record(FLAGS_value_size, 'a');
    uint64_t start = env->NowMicros();
    for (int i = 0; i < FLAGS_num; i++) {
        log_->AddRecord(record);
        // byte_written += FLAGS_value_size;
    }
    logfile_->Flush();
    uint64_t duration = env->NowMicros() - start;
    
    fprintf(stdout, "\n======= Speed %f Kops/s (%d). %f MB/s\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, FLAGS_num * FLAGS_value_size * 0.95367 / duration);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
