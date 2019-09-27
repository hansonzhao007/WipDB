// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "kv/options.h"

#include "kv/comparator.h"
#include "kv/env.h"

namespace kv {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(nullptr),
      write_buffer_size(4<<20),
      max_open_files(1000),
      block_cache(nullptr),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(64<<20),
      compression(kNoCompression),
      reuse_logs(false),
      wal_log(true),
      filter_policy(nullptr) {
}

}  // namespace kv
