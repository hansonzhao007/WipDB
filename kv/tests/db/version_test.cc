#include "db/memtable.h"
#include "kv/write_batch.h"
#include "db/write_batch_internal.h"
#include "util/color.h"
#include "db/dbformat.h"
#include "kv/db.h"
#include "db/db_impl.h"
#include "kv/env.h"
#include "kv/iterator.h"
#include "kv/status.h"
#include "db/version_edit.h"
#include "kv/table_builder.h"
#include "kv/table.h"
#include "util/coding.h"
#include "util/testharness.h"
#include "db/version_set.h"
#include "gtest/gtest.h"
#include "table/format.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include <string>
#include <iostream>

using namespace kv;


class VersionTest: public ::testing::Test {
public:
    VersionTest(): 
        dbname_("/tmp/versiontest"),
        options_(Options()),
        internal_comparator_(options_.comparator) {
        
        table_cache_ = new TableCache(dbname_, Options(), 1000);
        versions_    = new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);
        
    }
    ~VersionTest() {
    }
    std::string dbname_;
    Options options_;
    InternalKeyComparator internal_comparator_;
    TableCache* table_cache_;
    VersionSet* versions_;


};



int main(int argc, char** argv) {
   // ::testing::internal::CaptureStdout();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
