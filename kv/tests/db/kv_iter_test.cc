#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <ftw.h>

#include "db/memtable.h"
#include "kv/write_batch.h"
#include "db/write_batch_internal.h"
#include "util/color.h"
#include "db/dbformat.h"
#include "kv/db.h"
#include "kv/kv.h"
#include "kv/options.h"
#include "db/db_impl.h"
#include "kv/env.h"
#include "kv/iterator.h"
#include "kv/status.h"
#include "db/version_edit.h"
#include "kv/table_builder.h"
#include "kv/table.h"
#include "util/coding.h"
// #include "util/testharness.h"
#include "db/version_set.h"
#include "gtest/gtest.h"
#include "table/format.h"
#include "util/random.h"
#include "util/trace.h"
using namespace kv;

int main() {
    Options options_;
    options_.create_if_missing = true; 
    options_.write_buffer_size = 64 << 10;
    options_.filter_policy = NewBloomFilterPolicy(10);
    options_.create_if_missing = true;
    std::string dbname_ = "/tmp/kvtest";

    KV* db;
    vector<std::string> pivots;
    int partition = 5;
    for (uint64_t i = 1; i <= partition; ++i) {
        char key[100];
        snprintf(key, sizeof(key), "%020llu", (unsigned long long) kRANDOM_RANGE/partition * i);
        Slice skey(key);
        pivots.push_back(skey.ToString());
    }

    Status s = KV::Open(options_, dbname_, pivots,&db);
    Env::Default()->IncBackgroundThreadsIfNeeded(1, kv::Env::Priority::LOW);
    Env::Default()->IncBackgroundThreadsIfNeeded(1, kv::Env::Priority::HIGH);

    uint64_t num = 100;
    Random random(201);
    uint64_t step = kRANDOM_RANGE / num;
    for(uint64_t i = 0; i < num; ++i) {
        char key[100];
        snprintf(key, sizeof(key), "%020llu", (unsigned long long) i * step);
        Slice skey(key);
        Status s = db->Put(WriteOptions(), key, "value"+std::to_string(i));
        // fprintf(stdout, "Insert key: %s\n", key);
    }

    for (int i = 0; i < 2; ++i) {
        printf("=========== \n");
        auto* iter = db->NewIterator(ReadOptions());
        int count = 0;
        iter->SeekToFirst();
        while(iter->Valid() && count < 10) {
            // fprintf(stdout, "%s: %s\n", iter->key().ToString().c_str(), iter->value().ToString().c_str());
            iter->Next();
            count++;
        }
        delete iter;
        iter = nullptr;

    }
    
    return 0;
}