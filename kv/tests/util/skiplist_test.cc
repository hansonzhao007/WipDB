#include "util/skiplist.h"
#include "gtest/gtest.h"


#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_set>
#include <set>

#include "kv/env.h"
#include "util/debug.h"
#include "util/random.h"
#include "util/generator.h"

namespace kv {


struct Comparator {
  int operator()(const uint64_t& a, const uint64_t& b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

struct Key {
  uint64_t val;
  uint64_t seq;
  Key(uint64_t v): val(v) {
    seq = rand() % 1000;
  }
};

struct KeyComparator {
  int operator()(const struct Key& a, const struct Key& b) const {
    if (a.val < b.val) {
      return -1;
    } else if (a.val > b.val) {
      return +1;
    } else {
      if (a.seq < b.seq) return -1;
      else if (a.seq > b.seq) return +1;
      else return 0;
    }
  }
};

class SkipTest { };

TEST(SkipTest, Empty) {
  Arena arena;
  Comparator cmp;
  SkipList<uint64_t, Comparator> list(cmp, &arena);
  ASSERT_TRUE(!list.Contains(10));

  SkipList<uint64_t, Comparator>::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  iter.Seek(100);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
}

TEST(SkipTest, InsertAndLookup) {
  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<uint64_t> keys;
  Comparator cmp;
  Arena arena;
  SkipList<uint64_t, Comparator> list(cmp, &arena);
  for (int i = 0; i < N; i++) {
    uint64_t key = rnd.Next() % R;
    if (keys.insert(key).second) {
      list.Insert(key);
    }
  }

  for (int i = 0; i < R; i++) {
    if (list.Contains(i)) {
      ASSERT_EQ(keys.count(i), 1);
    } else {
      ASSERT_EQ(keys.count(i), 0);
    }
  }

  // Simple iterator tests
  {
    SkipList<uint64_t, Comparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());

    iter.Seek(0);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), iter.key());
  }

  // Forward iteration test
  for (int i = 0; i < R; i++) {
    SkipList<uint64_t, Comparator>::Iterator iter(&list);
    iter.Seek(i);

    // Compare against model iterator
    std::set<uint64_t>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  {
    SkipList<uint64_t, Comparator>::Iterator iter(&list);
    iter.SeekToLast();

    // Compare against model iterator
    for (std::set<uint64_t>::reverse_iterator model_iter = keys.rbegin();
         model_iter != keys.rend();
         ++model_iter) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*model_iter, iter.key());
      iter.Prev();
    }
    ASSERT_TRUE(!iter.Valid());
  }
}



}  // namespace kv

int main(int argc, char **argv) {
  // ::testing::internal::CaptureStdout();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
