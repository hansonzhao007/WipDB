// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "port/port.h"
#include "kv/slice_transform.h"
#include "kv/memtablerep.h"

namespace kv {

class HashCuckooRepFactory : public MemTableRepFactory {
 public:
  // maxinum number of hash functions used in the cuckoo hash.
  static const unsigned int kMaxHashCount = 10;

  explicit HashCuckooRepFactory(size_t write_buffer_size,
                                size_t average_data_size,
                                unsigned int hash_function_count)
      : write_buffer_size_(write_buffer_size),
        average_data_size_(average_data_size),
        hash_function_count_(hash_function_count) {}

  virtual ~HashCuckooRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& compare, Arena* allocator,
      const SliceTransform* transform, Logger* logger) override;

  virtual const char* Name() const override { return "HashCuckooRepFactory"; }

 private:
  size_t write_buffer_size_;
  size_t average_data_size_;
  const unsigned int hash_function_count_;
};
}  // namespace rocksdb

