#include "util/arena.h"
#include "gtest/gtest.h"
#include "util/color.h"
#include "db/memtable.h"
#include "util/hpblock.h"
#include "util/trace.h"
#include "kv/env.h"
#include "util/perfsvg.h"
#include "gflags/gflags.h"
#include "util/hash.h"

#include <stdint.h>
#include <limits.h>
#include <iostream>
#include <unordered_map>

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

using namespace std;
using namespace kv;


DEFINE_int64(num, 100000, "Number of key/values to place in database");
DEFINE_int64(partition, 100, "Number of memtables");
DEFINE_int64(value_size, 100, "value size");

DEFINE_int64(
    write_buffer_size, 256,
    "write_buffer_size parameter to pass into NewHashCuckooRepFactory");

DEFINE_int64(
    average_data_size, 64,
    "average_data_size parameter to pass into NewHashCuckooRepFactory");

DEFINE_int64(
    hash_function_count, 4,
    "hash_function_count parameter to pass into NewHashCuckooRepFactory");

namespace std {

  template <>
  struct hash<kv::Slice>
  {
    std::size_t operator()(const kv::Slice& k) const
    {
      // Compute individual hash values for first,
      // second and third and combine them using XOR
      // and bit shifting:
      return (std::size_t) kv::Hash(k.data(), k.size(), 0);
    }
  };
}


// TEST(MEM, TableIter) {
//     const int kNumPerPartition = FLAGS_num / FLAGS_partition;
//     Env* env = Env::Default();
//     Trace* trace = new TraceUniform(5143);
//     const InternalKeyComparator internal_comparator(Options().comparator);
//     MemTable* table = new MemTable(internal_comparator, nullptr, MemOptions());;
//     const string value(10, 'a');
//     // 生成 sequence 序列，决定每次往哪个 partition memtable 插入
//     vector<int> seq(FLAGS_num);
//     vector<std::string> keys(FLAGS_num);
//     for (int i = 0; i < FLAGS_num; ++i) {
//         const int k = (trace->Next() % FLAGS_num);
//         char key[100];
//         snprintf(key, sizeof(key), "%08d%08d", k, i);
//         table->Add(i, kTypeValue, key, value + std::to_string(i));
//     }
//     Arena arena;
//     Iterator* iter = table->NewIterator(ReadOptions(), &arena);
//     iter->SeekToFirst();
//     while(iter->Valid()) {
//         InternalKey key; key.DecodeFrom(iter->key());
//         fprintf(stdout, "%s: %s\n", key.user_key().ToString().c_str(), iter->value().ToString().c_str());
//         iter->Next();
//     }
    
// }

// TEST(MEM, HugePage) {
//     const int kNumPerPartition = FLAGS_num / FLAGS_partition;
//     debug_perf_ppid();
//     Env* env = Env::Default();
//     Trace* trace = new TraceUniform(134);
// 	// build 512MB huge page space
// 	// 65536 = 64KB. 64KB * 4 * 2048 partition = 512MB
// 	HugePage hp_seq(FLAGS_write_buffer_size, FLAGS_partition), hp_rnd(FLAGS_write_buffer_size, FLAGS_partition);
//     vector<char*> blocks(FLAGS_partition);
//     vector<MemTable*> tables_huge_seq(FLAGS_partition), tables_huge_rnd(FLAGS_partition);
//     vector<MemTable*> tables_new(FLAGS_partition);
//     const InternalKeyComparator internal_comparator(Options().comparator);
//     for (int i = 0; i < FLAGS_partition; ++i) {
//         tables_huge_seq[i] = new MemTable(internal_comparator, hp_seq.page_blocks_[i]);
//         tables_huge_rnd[i] = new MemTable(internal_comparator, hp_rnd.page_blocks_[i]);
//     }

//     // 生成 sequence 序列，决定每次往哪个 partition memtable 插入
//     vector<int> seq(FLAGS_num);
//     vector<std::string> keys(FLAGS_num);

//     for (int i = 0; i < FLAGS_num; ++i) {
//         seq[i] = random() % FLAGS_partition;
//         const int k = (trace->Next() % FLAGS_num);
//         char key[100];
//         snprintf(key, sizeof(key), "%08d%08d", k, i);
//         keys[i] = std::string(key);
//     }

//     const string value(100, 'a');

//     uint64_t start, end;

//     debug_perf_switch();
//     start = env->NowMicros();
//     for (int i = 0; i < FLAGS_num; ++i) {
//         tables_huge_rnd[seq[i]]->Add(i, kTypeValue, keys[i], value);
//     }
//     end = env->NowMicros();
//     fprintf(stdout, "Hugepage random write time: %llu\n", (unsigned long long) end - start);

//     debug_perf_switch();
//     start = env->NowMicros();
//     for (int i = 0; i < FLAGS_num; ++i) {
//         tables_huge_seq[i / kNumPerPartition]->Add(i, kTypeValue, keys[i], value);
//     }
//     end = env->NowMicros();
//     fprintf(stdout, "Hugepage sequen write time: %llu\n", (unsigned long long) end - start);

// }

// namespace std {

//   template <>
//   struct hash<Slice>
//   {
//     std::size_t operator()(const Slice& k) const
//     {
//       // Compute individual hash values for first,
//       // second and third and combine them using XOR
//       // and bit shifting:
//       return (std::size_t) Hash(k.data(), k.size(), 0);
//     }
//   };
// }

TEST(MEM, MapSlice) {
    Env* env = Env::Default();
    Trace* trace = new TraceUniform(134);
    
    unordered_map<Slice, const char*> table;
    vector<std::string> keys(10);
    vector<std::string> values(10);
    for (int i = 0; i < 10; ++i) {
        const int k = (trace->Next() % 100000);
        char key[100];
        snprintf(key, sizeof(key), "%08d%08d", k, i);
        keys[i] = std::string(key);
        values[i] = std::string(key) + " - " + std::to_string(i);
    }
    uint64_t duration = 0;
    uint64_t start, end;
    start = env->NowMicros();
    for (int i = 0; i < 10; ++i) {
        table[keys[i]] = values[i].data();
    }
    end = env->NowMicros();
    duration = end - start;


    for (auto e: table) {
        printf("key: %s. value: %s\n", e.first.ToString().c_str(), e.second);
    }
    // fprintf(stdout, "Newpage random write time: %llu\n", (unsigned long long) duration);
    fprintf(stdout, "\n======= Unordered Slice Speed %f Kops/s (%d). %f MB/s\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, FLAGS_num * FLAGS_value_size * 0.95367 / duration);
    
}

TEST(MEM, TestMap) {
    const int kNumPerPartition = FLAGS_num / FLAGS_partition;
    Env* env = Env::Default();
    Trace* trace = new TraceUniform(134);
    
    vector<unordered_map<Slice, KeyHandle>> tables(FLAGS_partition, unordered_map<Slice, KeyHandle>());
    vector<Arena> arenas(FLAGS_partition);
    vector<int> seq(FLAGS_num);
    vector<Slice> keys(FLAGS_num);
    vector<KeyHandle> values(FLAGS_num);
    for (int i = 0; i < FLAGS_num; ++i) {
        seq[i] = random() % FLAGS_partition;
        const int k = (trace->Next() % FLAGS_num);
        char ckey[100];
        snprintf(ckey, sizeof(ckey), "%08d%08d", k, i);

        Slice key(ckey, 16);
        Slice value("value: " + std::to_string(i));
        uint32_t key_size = static_cast<uint32_t>(key.size());
        uint32_t val_size = static_cast<uint32_t>(value.size());
        uint32_t internal_key_size = key_size + 8;
        const uint32_t encoded_len = VarintLength(internal_key_size) +
                                    internal_key_size + VarintLength(val_size) +
                                    val_size;
        char* buf = arenas[seq[i]].Allocate(encoded_len);
        KeyHandle handle = (KeyHandle) buf;
        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);
        Slice key_slice(p, key_size);
        keys[i] = key_slice;
        p += key_size;
        EncodeFixed64(p, (i << 8) | kTypeValue);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, value.data(), val_size);
        assert(p + val_size == buf + encoded_len);
        values[i] = handle;
    }

    const string value(FLAGS_value_size, 'a');
    uint64_t duration = 0;
    uint64_t start, end;

    start = env->NowMicros();
    for (int i = 0; i < FLAGS_num; ++i) {
        tables[seq[i]][keys[i]] = values[i];
    }
    end = env->NowMicros();
    duration = end - start;
    // fprintf(stdout, "Newpage random write time: %llu\n", (unsigned long long) duration);
    fprintf(stdout, "\n======= Random Speed %f Kops/s (%d). %f MB/s\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, FLAGS_num * FLAGS_value_size * 0.95367 / duration);
    

    // for (auto e: tables[0]) {
    //     printf("key: %s. value: %s\n", e.first.ToString().c_str(), e.second);
    // }
}


// TEST(MEM, NewPage) {
//     const int kNumPerPartition = FLAGS_num / FLAGS_partition;
//     Env* env = Env::Default();
//     Trace* trace = new TraceUniform(134);
//     MemOptions mem_options;
//     mem_options.write_buffer_size = FLAGS_write_buffer_size;
//     mem_options.average_data_size = FLAGS_average_data_size;
//     mem_options.hash_function_count = FLAGS_hash_function_count;

//     vector<MemTable*> tables_new_seq(FLAGS_partition), tables_new_rnd(FLAGS_partition);
//     const InternalKeyComparator internal_comparator(Options().comparator);
//     for (int i = 0; i < FLAGS_partition; ++i) {       
//         tables_new_seq[i]  = new MemTable(internal_comparator, nullptr,  mem_options);
//         tables_new_rnd[i]  = new MemTable(internal_comparator, nullptr,  mem_options);
//     }

//     // 生成 sequence 序列，决定每次往哪个 partition memtable 插入
//     vector<int> seq(FLAGS_num);
//     vector<std::string> keys(FLAGS_num);
//     for (int i = 0; i < FLAGS_num; ++i) {
//         seq[i] = random() % FLAGS_partition;
//         const int k = (trace->Next() % FLAGS_num);
//         char key[100];
//         snprintf(key, sizeof(key), "%08d%08d", k, i);
//         keys[i] = std::string(key);
//     }

//     const string value(FLAGS_value_size, 'a');
//     uint64_t duration = 0;
//     uint64_t start, end;

//     start = env->NowMicros();
//     for (int i = 0; i < FLAGS_num; ++i) {
//         tables_new_rnd[seq[i]]->Add(i, kTypeValue, keys[i], value);
//     }
//     end = env->NowMicros();
//     duration = end - start;
//     // fprintf(stdout, "Newpage random write time: %llu\n", (unsigned long long) duration);
//     fprintf(stdout, "\n======= Random Speed %f Kops/s (%d). %f MB/s\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, FLAGS_num * FLAGS_value_size * 0.95367 / duration);
    
//     start = env->NowMicros();
//     for (int i = 0; i < FLAGS_num; ++i) {
//         tables_new_seq[i / kNumPerPartition]->Add(i, kTypeValue, keys[i], value);
//     }
//     end = env->NowMicros();
//     duration = end - start;
//     // fprintf(stdout, "Newpage sequen write time: %llu\n", (unsigned long long) duration);

//     fprintf(stdout, "\n======= Seq Speed %f Kops/s (%d). %f MB/s\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, FLAGS_num * FLAGS_value_size * 0.95367 / duration);
// }




int main(int argc, char **argv) {
    // ParseCommandLineFlags(&argc, &argv, true);
    // const int kNumPerPartition = FLAGS_num / FLAGS_partition;
    // Env* env = Env::Default();
    // Trace* trace = new TraceUniform(134);
    // MemOptions mem_options;
    // mem_options.write_buffer_size = FLAGS_write_buffer_size;
    // mem_options.average_data_size = FLAGS_average_data_size;
    // mem_options.hash_function_count = FLAGS_hash_function_count;
    // mem_options.prefix_extractor.reset(kv::NewFixedPrefixTransform(8));
    
    // vector<MemTable*> tables_new_rnd(FLAGS_partition);
    // const InternalKeyComparator internal_comparator(Options().comparator);
    // for (int i = 0; i < FLAGS_partition; ++i) {       
    //     tables_new_rnd[i]  = new MemTable(internal_comparator, nullptr,  mem_options);
    // }

    // // 生成 sequence 序列，决定每次往哪个 partition memtable 插入
    // vector<int> seq(FLAGS_num);
    // vector<std::string> keys(FLAGS_num);
    // for (int i = 0; i < FLAGS_num; ++i) {
    //     seq[i] = random() % FLAGS_partition;
    //     const int k = (trace->Next() % FLAGS_num);
    //     char key[100];
    //     snprintf(key, sizeof(key), "%010d%010d", k, i);
    //     keys[i] = std::string(key);
    // }

    // const string value(FLAGS_value_size, 'a');
    // uint64_t duration = 0;
    // uint64_t start, end;

    // start = env->NowMicros();
    // for (int i = 0; i < FLAGS_num; ++i) {
    //     tables_new_rnd[random() % FLAGS_partition]->Add(i, kTypeValue, keys[i], value);
    // }
    // end = env->NowMicros();
    // duration = end - start;
    // fprintf(stdout, "\n======= Random Speed %f Kops/s (%d). %f MB/s\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, FLAGS_num * FLAGS_value_size * 0.95367 / duration);
    
  ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}