#include <inttypes.h>
#include <chrono>
#include <limits>
#include <algorithm>
#include <stdint.h>
#include <stdio.h>
#include <cerrno>
#include <sys/stat.h>
#include <set>
#include <string>
#include <vector>
#include <cmath>

#include "kv/kv.h"
#include "kv/write_batch.h"
#include "kv/memtablerep.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "util/trace.h"
#include "util/color.h"
#include "util/hash_table.h"
#include "util/testutil.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;
using namespace kv;

DEFINE_int64(num, 100000, "Number of key/values to place in database");
DEFINE_int64(partition, 10, "Number of memtables");
DEFINE_int64(value_size, 100, "value size");
DEFINE_int64(write_buffer_size, 2 << 20, "bucket mem size");


// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, 0.5, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};


typedef struct Bucket {
    Arena arena;
    Options options;
    MemTableRepFactory* memtable_factory;
    InternalKeyComparator internal_key_comp;
    MemTable::KeyComparator key_comp;
    MemTableRep* table;
    HashTable* hashtable;
    Bucket(): 
        arena(new HugePageBlock(FLAGS_write_buffer_size * 2 )),
        memtable_factory(new SkipListFactory()),
        internal_key_comp(options.comparator),
        key_comp(internal_key_comp),
        table(memtable_factory->CreateMemTableRep(key_comp, &arena, options.prefix_extractor.get(), nullptr)),
        hashtable(new HashTable()) {
    }

    const char* GenerateEntry(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
        uint32_t key_size = static_cast<uint32_t>(key.size());
        uint32_t val_size = static_cast<uint32_t>(value.size());
        uint32_t internal_key_size = key_size + 8;
        const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size + 
                                    VarintLength(val_size) + val_size;
        char* buf = nullptr;
        KeyHandle handle = table->Allocate(encoded_len, &buf);

        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);
        Slice key_slice(p, key_size);
        p += key_size;
        EncodeFixed64(p, (s << 8) | type);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, value.data(), val_size);
        assert(p + val_size == buf + encoded_len);
        return buf;
    };
} bucket_t;

class HashTableTest : public testing::Test {
public:
    std::vector<bucket_t> buckets;
    std::map<Slice, KeyHandle> mmap;
    Env* env;
    Trace* trace;
    RandomGenerator gen;
    HashTableTest()
        {
        env = Env::Default();
        trace = new TraceUniform(134);
    }

    
};

TEST_F(HashTableTest, MultipleHashTableInsertionSpeed) {
    vector<bucket_t> buckets(FLAGS_partition);

    // choose which partition
    vector<int> seq(FLAGS_num);
    for (int i = 0; i < FLAGS_num; ++i) {
        seq[i] = random() % FLAGS_partition;
    }
    
    // test hash
    int collision = 0;
    auto start = env->NowMicros();
    for (uint64_t i = 0; i < FLAGS_num; ++i) {
        const uint64_t k = (trace->Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016" PRIu64 "", k);
        const char* value = buckets[seq[i]].GenerateEntry(i, kTypeValue, key, gen.Generate(FLAGS_value_size));
        if(!buckets[seq[i]].hashtable->Put(key, value)) {
            ++collision;
        }
    }

    auto duration = env->NowMicros() - start;
    fprintf(stdout, "Bucket, %d, Hashtable_Random_Insertion_Speed, %f , Kops/s (num %d each bucket). collision: %d. miss ratio: %f%% === \n", 
                (int) FLAGS_partition, 
                (double)FLAGS_num / duration * 1000.0,  
                (int) FLAGS_num / (int)FLAGS_partition, 
                (int) collision, 
                (double) collision / FLAGS_num * 100);
}



int main(int argc, char **argv) {
    ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}