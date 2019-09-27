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
#include "db/versionkv.h"
#include <string>
#include <iostream>

using namespace kv;


class VersionSetKVTest: public ::testing::Test {
public:
    VersionSetKVTest(): 
        options_(Options()),
        vset_("test", &options_) {

    }
    ~VersionSetKVTest() {
    }
    Options options_;
    VersionSetKV vset_;
};

TEST_F(VersionSetKVTest, Empty) {
  fprintf(stdout, "Version count: %d\n", vset_.VersionCount());  
}


TEST_F(VersionSetKVTest, AppendVersion) {
  VersionKVEdit edit;
  for (int i = 0; i < 10; i++) {
    Bucket* n = new Bucket(std::to_string(i));
    edit.AddBucket(n);
  }
  vset_.Apply(&edit);
  fprintf(stdout, "%s\n", vset_.current()->BucketsInfo().c_str());
  fprintf(stdout, "Version count after append new version: %d\n", vset_.VersionCount()); 
}


TEST_F(VersionSetKVTest, AppendVersionWhileRefOldVersion) {
  VersionKVEdit edit;
  for (int i = 0; i < 10; i++) {
    Bucket* n = new Bucket(std::to_string(i));
    edit.AddBucket(n);
  }
  auto v = vset_.current();
  v->Ref();
  fprintf(stdout, "%s\n", vset_.current()->BucketsInfo().c_str());
  vset_.Apply(&edit);
  fprintf(stdout, "%s\n", vset_.current()->BucketsInfo().c_str());
  fprintf(stdout, "Version count after append new version: %d\n", vset_.VersionCount()); 

  v->Unref();
  fprintf(stdout, "Version count after old version unref: %d\n", vset_.VersionCount()); 
  fprintf(stdout, "%s\n", vset_.current()->BucketsInfo().c_str());
  
}

int main(int argc, char** argv) {
   // ::testing::internal::CaptureStdout();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
