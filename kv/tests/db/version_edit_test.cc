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
// #include "util/testharness.h"
#include "db/version_set.h"
#include "gtest/gtest.h"
#include "table/format.h"
#include <string>
#include <iostream>

using namespace kv;


class VersionEditTest: public ::testing::Test {
public:
    VersionEditTest() {
        Slice name = "test.comparator";
        v_edit_.SetComparatorName(name);
        v_edit_.SetLastSequence(111);
        v_edit_.SetLogNumber(222);
        v_edit_.SetNextFile(333);
        v_edit_.SetPrevLogNumber(0);
        v_edit_.SetBitMaps(0x80c0e0f0f8fcfeff);
    }
    ~VersionEditTest() {
    }

    VersionEdit v_edit_;
};

TEST_F(VersionEditTest, KVEncodeDecode) {
  string encode;
  v_edit_.EncodeTo(&encode);
  GTEST_COUT << v_edit_.DebugString() << std::endl;

  VersionEdit edit;
  Status s = edit.DecodeFrom(encode);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(v_edit_.DebugString(), edit.DebugString());
}

int main(int argc, char** argv) {
   // ::testing::internal::CaptureStdout();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
