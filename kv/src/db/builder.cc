// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "kv/db.h"
#include "kv/env.h"
#include "kv/iterator.h"
#include "util/file_reader_writer.h"

namespace kv {

Status BuildTableKV(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  // build a level i table at offset 
  Status s;
  meta->file_size = 0;
  meta->count = 0;
  
  iter->SeekToFirst();

  // create file
  std::string fname = TableFileNameKV(dbname, meta->number);
  EnvOptions env_options;
  env_options.use_direct_writes = options.direct_io;

  if (iter->Valid()) {
    WritableFile* file;
    // get file handler
    s = env->NewWritableFile(fname, &file, env_options);
    unique_ptr<WritableFileWriter> file_writer(
            new WritableFileWriter(std::move(file), fname, env_options, true));
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file_writer.get());

    int kn = 0;
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      kn++;
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
    }
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      
      // record count and median
      meta->count = kn;
      iter->SeekToFirst();
      kn = kn / 2;
      while (kn--) {
        iter->Next();
      }
      meta->median.DecodeFrom(iter->key());

      assert(meta->file_size > 0);
    }
    delete builder;

    if (s.ok()) {
      s = file_writer->Sync(false);
    }

    if (s.ok()) {
      s = file_writer->Close();
    }
    
    delete file;
    file = nullptr;

    // Verity table 
    if (s.ok()) {
      uint64_t start_micros = env->NowMicros();
      // Verify that the table is usable
      Iterator* it = table_cache->NewIteratorKV(ReadOptions(),
                                              meta);
      s = it->status();
      
      delete it;

      Log(options.info_log, "Level-0 Verify time: %llu. %s (%llu keys). Cache used: %d", (unsigned long long)env->NowMicros() - start_micros, s.ToString().c_str(), (unsigned long long)meta->count, (int)table_cache->cache_->TotalCharge());
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;                   
}

}  // namespace leveldb
