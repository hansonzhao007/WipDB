// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <cstdint>
#include <string>

#include "kv/comparator.h"
#include "kv/slice.h"
#include "util/no_destructor.h"

namespace kv {

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const override {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }

  virtual bool Equal(const Slice& a, const Slice& b) const override {
    return a == b;
  }

  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t start_byte = static_cast<uint8_t>((*start)[diff_index]);
      uint8_t limit_byte = static_cast<uint8_t>(limit[diff_index]);
      if (start_byte >= limit_byte) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        return;
      }
      assert(start_byte < limit_byte);

      if (diff_index < limit.size() - 1 || start_byte + 1 < limit_byte) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
      } else {
        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diff_index++;

        while (diff_index < start->size()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if (static_cast<uint8_t>((*start)[diff_index]) <
              static_cast<uint8_t>(0xff)) {
            (*start)[diff_index]++;
            start->resize(diff_index + 1);
            break;
          }
          diff_index++;
        }
      }
      assert(Compare(*start, limit) < 0);
    }
  }

  virtual void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }

  virtual bool IsSameLengthImmediateSuccessor(const Slice& s,
                                              const Slice& t) const override {
    if (s.size() != t.size() || s.size() == 0) {
      return false;
    }
    size_t diff_ind = s.difference_offset(t);
    // same slice
    if (diff_ind >= s.size()) return false;
    uint8_t byte_s = static_cast<uint8_t>(s[diff_ind]);
    uint8_t byte_t = static_cast<uint8_t>(t[diff_ind]);
    // first different byte must be consecutive, and remaining bytes must be
    // 0xff for s and 0x00 for t
    if (byte_s != uint8_t{0xff} && byte_s + 1 == byte_t) {
      for (size_t i = diff_ind + 1; i < s.size(); ++i) {
        byte_s = static_cast<uint8_t>(s[i]);
        byte_t = static_cast<uint8_t>(t[i]);
        if (byte_s != uint8_t{0xff} || byte_t != uint8_t{0x00}) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  virtual bool CanKeysWithDifferentByteContentsBeEqual() const override {
    return false;
  }
};

}  // namespace

const Comparator* BytewiseComparator() {
  static BytewiseComparatorImpl bytewise;
  return &bytewise;
}

}  // namespace leveldb
