#include "util/hash_table.h"
#include <inttypes.h>
#include <chrono>
#include <limits>
#include <algorithm>

#include "kv/kv.h"

#include <stdint.h>
#include <stdio.h>
#include <cerrno>
#include <sys/stat.h>
#include <set>
#include <string>
#include <vector>
#include <cmath>
#include "kv/write_batch.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "util/trace.h"
#include "util/color.h"
#include "kv/memtablerep.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

using namespace kv;
using std::vector;

DEFINE_int64(num, 10000, "Number of key/values to place in database");
DEFINE_int64(partition, 1000, "Number of partitions");

// TODO(yhchiang): the rate will not be accurate when we run test in parallel.
class IterationHandler: public HashHandler {
public:
    void Action(KeyHandle entry) override{
        const char* e = (char*) entry;
        Slice key_slice = GetLengthPrefixedSlice(e);
        Slice value_slice = GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
        InternalKey key; key.DecodeFrom(key_slice);
        fprintf(stdout, "Key: %s - Value: %s\n", key.user_key().ToString().c_str(), value_slice.ToString().c_str());
    }
};

class MapHandler: public HashHandler {
public:
    explicit MapHandler(std::map<Slice, KeyHandle>* array_ptr) {
        array_ = array_ptr;
    }
    void Action(KeyHandle entry) override{
        const char* e = (char*) entry;
        Slice key_slice = GetLengthPrefixedSlice(e);
        (*array_)[key_slice] = entry;
    }
private:
    std::map<Slice, KeyHandle>* array_;
};

class ArrayHandler: public HashHandler {
public:
    explicit ArrayHandler (std::vector<ArrayNode>* _array) {
        array = _array;
    }
    void Action (KeyHandle entry) override {
        Slice key_slice = GetLengthPrefixedSlice((char*)entry);
        array->push_back(ArrayNode(key_slice, entry));
    }
private:
    std::vector<ArrayNode>* array;
};

class HashTableTest : public testing::Test {
public:
    Arena arena;
    Options options;
    MemTableRepFactory* memtable_factory;
    InternalKeyComparator internal_key_comp;
    MemTable::KeyComparator key_comp;
    MemTableRep* table;
    std::map<Slice, KeyHandle> mmap;
    std::vector<ArrayNode> marray;
    Env* env;
    Trace* trace;
    vector<std::string> keys;
    vector<const char*> values;
    HashTable::IterHandler* handler; 
    IterationHandler iter_handler;
    MapHandler map_handler;
    ArrayHandler array_handler;
    HashTableTest(): 
        memtable_factory(new SkipListFactory()),
        internal_key_comp(options.comparator),
        key_comp(internal_key_comp),
        table(memtable_factory->CreateMemTableRep(key_comp, &arena, options.prefix_extractor.get(), nullptr)),
        handler(new HashTable::IterHandler(table)),
        map_handler(&mmap),
        array_handler(&marray)
        {
        env = Env::Default();
        trace = new TraceUniform(134);
        keys.resize(FLAGS_num);
        values.resize(FLAGS_num);
        for (int i = 0; i < FLAGS_num; ++i) {
            const int k = (trace->Next() % 10000000);
            char key[100];
            snprintf(key, sizeof(key), "%08d%08d", i, k);
            keys[i] = std::string(key);
            values[i] = GenerateEntry(i, kTypeValue, keys[i], "value" + std::string(key));
        }
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
};


TEST_F(HashTableTest, SizeofSlot) {
    printf("[----------] Slot size: %lu.\n", sizeof(HashSlot));
}


TEST_F(HashTableTest, CreateSpeed) {    
    auto start = env->NowMicros();
    for (int i = 0; i < 1000; ++i) {
        HashTable* hashtable = new HashTable();
        delete hashtable;
    }
    auto duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Hashtable Creation Speed %f us/per.\n", (double)duration / 1000.0);
    
}


TEST_F(HashTableTest, ResetSpeed) {    
    vector<HashTable*> tables;
    auto start = env->NowMicros();
    for (int i = 0; i < 1000; ++i) {
        tables.push_back(new HashTable());
    }
    auto duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Hashtable Creation Speed %f us/per.\n", (double)duration / 1000.0);

    start = env->NowMicros();
    for (int i = 0; i < 1000; ++i) {
        tables[i]->Reset();
    }
    duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Hashtable Reset Speed %f us/per.\n", (double)duration / 1000.0);
    
}

TEST_F(HashTableTest, DecodeKeyAndValue) {
    Slice key1("000000001"), value1("value1");
    Slice key2("000000002"), value2("value2");

    const char* entry1 = GenerateEntry(1, kTypeValue, key1, value1);
    const char* entry2 = GenerateEntry(2, kTypeValue, key2, value2);
    
    Slice dk1 = HashTable::DecodeKeyFromEntry(entry1);
    Slice dv1 = HashTable::DecodeValueFromEntry(entry1);
    printf("[----------] %s - %s. Decode: %s - %s\n", key1.ToString().c_str(), value1.ToString().c_str(), dk1.ToString().c_str(), dv1.ToString().c_str());
}


TEST_F(HashTableTest, SingleHashTableSimpleContainsTest) {
    HashTable* hashtable = new HashTable();
   
    vector<std::string> keys = {"0000000000001507", "0000000000001516", "0000000000001519"};
    vector<const char*> entries(3);
    for (int i = 0; i < 3; ++i) {
        entries[i] = GenerateEntry(i, kTypeValue, keys[i], "value" + std::to_string(i));
    }

    const char* entry = nullptr;
    for (int i = 0; i < 3; ++i) {
        // printf("key: %s", keys[i].c_str());
        if (!hashtable->Put(keys[i], entries[i])) {
            printf("[----------] Hash collition. %s\n", keys[i].c_str());
        }
        if (!hashtable->Get(keys[i], entry)) {
            printf("[----------] Error: not find inserted key %s\n", keys[i].c_str());
        }
    }
}


TEST_F(HashTableTest, SingleHashTableContainsTest) {
    HashTable* hashtable = new HashTable();

    const char* entry = nullptr;
    for (int i = 0; i < std::min((int)FLAGS_num, 65536); ++i) {
        // printf("key: %s", keys[i].c_str());
        if (!hashtable->Put(keys[i], values[i])) {
            printf("[----------] Hash collition. %s\n", keys[i].c_str());
        }
        if (!hashtable->Get(keys[i], entry)) {
            printf("[----------] Error: not find inserted key %s\n", keys[i].c_str());
        }
        else {
            Slice entry_val = HashTable::DecodeValueFromEntry(entry);
            ASSERT_EQ(entry_val.compare(HashTable::DecodeValueFromEntry(values[i])), 0);
        }
    }
}



TEST_F(HashTableTest, HashCollisionRate) {
    HashTable* hashtable = new HashTable();
    const char* entry = nullptr;
    int collision = 0;
    auto start = env->NowMicros();
    for (int i = 0; i < std::min((int)FLAGS_num, 65536); ++i) {
        // printf("key: %s", keys[i].c_str());
        if (!hashtable->Put(keys[i], values[i])) {
            // printf("[----------] Hash collition. %s\n", keys[i].c_str());
            collision++;
        }
    }
    auto duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Hashtable Collition Rate: %f %% (%d collision in %d entries). Insertion Speed %f kops/s \n", (double)collision/FLAGS_num * 100.0, collision, (int)FLAGS_num, (double)FLAGS_num / duration * 1000.0);
}


TEST_F(HashTableTest, HashCollisionRateWithMemCopy) {
    HashTable* hashtable = new HashTable();
    const char* entry = nullptr;
    int collision = 0;
    auto start = env->NowMicros();
    for (int i = 0; i < std::min((int)FLAGS_num, 65536); ++i) {
        SequenceNumber s = i;
        uint32_t key_size = static_cast<uint32_t>(keys[i].size());
        uint32_t val_size = static_cast<uint32_t>(100);
        uint32_t internal_key_size = key_size + 8;
        const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size + 
                                    VarintLength(val_size) + val_size;
        char* buf = nullptr;
        KeyHandle handle = table->Allocate(encoded_len, &buf);

        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, keys[i].data(), key_size);
        Slice key_slice(p, key_size);
        p += key_size;
        EncodeFixed64(p, (s << 8) | kTypeValue);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, values[i], val_size);
        assert(p + val_size == buf + encoded_len);

        if (!hashtable->Put(key_slice, buf)) {
            collision++;
        }
    }
    auto duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Hashtable Collition Rate: %f %% (%d collision in %d entries). Insertion Speed %f kops/s \n", (double)collision/FLAGS_num * 100.0, collision, (int)FLAGS_num, (double)FLAGS_num / duration * 1000.0);
}


TEST_F(HashTableTest, SingleHashTableContainsSpeed) {
    HashTable* hashtable = new HashTable();
    
    const char* entry = nullptr;
    int insert_collision = 0, read_missing = 0;
    for (int i = 0; i < std::min((int)FLAGS_num, 65536); ++i) {
        if(!hashtable->Put(keys[i], values[i])) {
            insert_collision++;
        }
    }

    auto start = env->NowMicros();
    for (int i = 0; i < std::min((int)FLAGS_num, 65536); ++i) {
        if(!hashtable->Get(keys[i], entry)) {
            read_missing++;
        }
    }
    auto duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Hashtable contains speed %f Kops/s (%d: collision: %d. read missing: %d).\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, insert_collision, read_missing);
}


// TEST_F(HashTableTest, HashTableIterator) {
//     HashTable* hashtable = new HashTable();
    
//     const char* entry = nullptr;
//     for (int i = 0; i < 10; ++i) {
//         // printf("key: %s", keys[i].c_str());
//         if (!hashtable->Put(keys[i], values[i])) {
//             printf("[----------] Hash collition. %s\n", keys[i].c_str());
//         }
//     }

//     hashtable->Iterate(&iter_handler);

// }



TEST_F(HashTableTest, HashTableMapSort) {
    HashTable* hashtable = new HashTable();
    
    const char* entry = nullptr;
    for (int i = 0; i < std::min(5000, (int)FLAGS_num); ++i) {
        // printf("key: %s", keys[i].c_str());
        if (!hashtable->Put(keys[i], values[i])) {
            printf("[----------] Hash collition. %s\n", keys[i].c_str());
        }
    }

    auto start = env->NowMicros();
    hashtable->Iterate(&map_handler);
    auto duration = env->NowMicros() - start;
    printf("Map sort time: %d us. Map size: %d\n", (int)duration, (int)mmap.size());
  
}




// TEST_F(HashTableTest, HashTableArraySort) {
//     HashTable* hashtable = new HashTable();
    
//     const char* entry = nullptr;
//     for (int i = 0; i < std::min(5000, (int)FLAGS_num); ++i) {
//         // printf("key: %s", keys[i].c_str());
//         if (!hashtable->Put(keys[i], values[i])) {
//             printf("[----------] Hash collition. %s\n", keys[i].c_str());
//         }
//     }

//     auto start = env->NowMicros();
//     hashtable->Iterate(&array_handler);
//     std::sort(marray.begin(), marray.end(), MemTable::ByArrayNode());
//     auto duration = env->NowMicros() - start;
//     printf("Array sort time: %d us. Array size: %d\n", (int)duration, (int)marray.size());
  
//     for (int i = 0; i < marray.size(); ++i) {
//         auto a = marray[i];
//         Slice ua(a.key.data(), a.key.size() - 8);
//         uint64_t anum = DecodeFixed64(ua.data() + ua.size()); // seq << 8 | kValueType
//         uint64_t aseq = anum >> 8;
//         uint64_t type = anum & 0xff;
//         printf("user key(%d)#%d - %d: %s\n", 
//         (int)ua.size(), 
//         (int)aseq,
//         (int)type,
//         ua.ToString().c_str());
//     }
// }

TEST_F(HashTableTest, HashTableToMemTable) {
    HashTable* hashtable = new HashTable();
    
    const char* entry = nullptr;
    for (int i = 0; i < std::min(5000, (int)FLAGS_num); ++i) {
        // printf("key: %s", keys[i].c_str());
        if (!hashtable->Put(keys[i], values[i])) {
            printf("[----------] Hash collition. %s\n", keys[i].c_str());
        }
    }

    auto start = env->NowMicros();
    hashtable->Iterate(handler);
    auto iter = table->GetIterator();
    iter->SeekToFirst();
    auto duration = env->NowMicros() - start;
    printf("MemTable sort time: %d us\n", (int)duration);

    // while (iter->Valid()) {
    //     Slice key_slice = GetLengthPrefixedSlice(iter->key());
    //     Slice value_slice = GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    //     InternalKey key; key.DecodeFrom(key_slice);
    //     fprintf(stdout, "Key: %s - Value: %s\n", key.user_key().ToString().c_str(), value_slice.ToString().c_str());
    //     iter->Next();
    // }
}



TEST_F(HashTableTest, MultipleHashTableInsertionSpeed) {
   
    vector<HashTable*> tables(FLAGS_partition);
    for(auto& t : tables) {
        t = new HashTable();
    }
  
    const char* entry = nullptr;
    vector<int> seq(FLAGS_num); // 选择进入哪个partition
    for (int i = 0; i < FLAGS_num; ++i) {
        seq[i] = random() % FLAGS_partition;
    }
    
    int collision = 0;
    auto start = env->NowMicros();
    for (int i = 0; i < FLAGS_num; ++i) {
        if(!tables[seq[i]]->Put(keys[i], values[i])) {
            ++collision;
        }
    }
    auto duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Random Insertion speed %f Kops/s (%d). collision: %d. miss ratio: %f%%\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, collision, (double) collision / FLAGS_num * 100);

    int read_missing = 0;
    start = env->NowMicros();
    for (int i = 0; i < FLAGS_num; ++i) {
        if(!tables[seq[i]]->Get(keys[i], entry)) {
            ++read_missing;
        }
    }
    duration = env->NowMicros() - start;
    fprintf(stdout, "[----------] ======= Random  contains speed %f Kops/s (%d). missing:  %d. miss ratio: %f%%\n", (double)FLAGS_num / duration * 1000.0, (int) FLAGS_num, read_missing, (double) read_missing / FLAGS_num * 100);
}


// TEST_F(HashTableTest, MultipleHashTableResetAfterCollision) {
   
//     vector<HashTable*> tables(FLAGS_partition);
//     for(auto& t : tables) {
//         t = new HashTable();
//     }
  
//     const char* entry = nullptr;
//     vector<int> seq(FLAGS_num); // 选择进入哪个partition
//     for (int i = 0; i < FLAGS_num; ++i) {
//         seq[i] = random() % FLAGS_partition;
//     }
    
//     int collision = 0;
//     auto start = env->NowMicros();
//     for (int i = 0; i < FLAGS_num; ++i) {
//         if(!tables[seq[i]]->Put(keys[i], values[i])) {
//             tables[seq[i]] = new HashTable();
//         }
//     }
//     auto duration = env->NowMicros() - start;
//     fprintf(stdout, "[----------] ======= Random Insertion speed %f Kops/s (%d).\n", (double)FLAGS_num / duration * 1000.0);
// }

int main(int argc, char** argv) {
    ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
