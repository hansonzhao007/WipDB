//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/instrumented_mutex.h"

namespace kv {
// namespace {
// Statistics* stats_for_report(Env* env, Statistics* stats) {
//   if (env != nullptr && stats != nullptr &&
//       stats->stats_level_ > kExceptTimeForMutex) {
//     return stats;
//   } else {
//     return nullptr;
//   }
// }
// }  // namespace

void InstrumentedMutex::Lock() {
  LockInternal();
}

void InstrumentedMutex::LockInternal() {
  mutex_.Lock();
}

void InstrumentedCondVar::Wait() {
  WaitInternal();
}

void InstrumentedCondVar::WaitInternal() {
  cond_.Wait();
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us) {
  return TimedWaitInternal(abs_time_us);
}

bool InstrumentedCondVar::TimedWaitInternal(uint64_t abs_time_us) {
  return cond_.TimedWait(abs_time_us);
}

}  // namespace rocksdb
