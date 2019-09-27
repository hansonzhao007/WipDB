#include "util/skiplist.h"
#include "gtest/gtest.h"


#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <iostream>
#include <unordered_set>
#include <set>

#include "kv/env.h"
#include "util/debug.h"
#include "util/random.h"
#include "util/trace.h"
#include "util/generator.h"
using namespace kv;




static int FLAGS_KEY_NUM = 1; // unit: million
static std::string FLAGS_PATTERN = "uniform";
Trace* FLAGS_Trace = new TraceUniform(123);
static uint64_t FLAGS_KEY_BASE_NUM = 1 * 1000000; // 1M
static uint64_t FLAGS_KEY_INTERVAL = 1;
vector<Trace*> traces;

enum WorkloadMode { kSingleWorkload, kMixWorkload};
// 0: single workload 1: mixed workload
static enum WorkloadMode FLAGS_WORDLOAD_MODE = kSingleWorkload;

struct Key {
  uint64_t val;
  uint64_t seq;
  Key(uint64_t v): val(v+1) {
      seq =  random_uint64();
    // seq = 0;
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


void SaveList(SkipList<Key, KeyComparator>& list, std::string filename) {
    FILE* fp = fopen (filename.c_str(), "w+");
    SkipList<Key, KeyComparator>::Iterator iter(&list);
    iter.SeekToFirst();
    while(iter.Valid()){
        fprintf(fp, "%llu,\n", (unsigned long long) iter.key().val);
        iter.Next();
    }
    fclose(fp);
}

void SaveListPivit(SkipList<Key, KeyComparator>& list, std::string filename, int numM) {
    FILE* fp = fopen (filename.c_str(), "w+");
    SkipList<Key, KeyComparator>::Iterator iter(&list);
    iter.SeekToFirst();
    int i = 0;
    while(iter.Valid()){
        ++i;
        if(i >= numM) { // 
            fprintf(fp, "%llu,\n", (unsigned long long) iter.key().val);
            i = 0;
        }
        iter.Next();
    }
    fclose(fp);
}

Trace* &PickTrace() {
    if(FLAGS_WORDLOAD_MODE == kSingleWorkload)
        return traces[0];
    else
        return traces[random() % traces.size()];
}

std::string WorkLoadName() {
    std::string workload_name;
    for(auto& trace: traces)
        workload_name += trace->gi_->get_type() + "_";
    return workload_name;
}
void PivitDraw(int key_num_in_million, enum WorkloadMode mode) {
    ::mkdir("data",0755);
    std::cout << traces.size() << " wordloads" << std::endl;
    for(auto& trace: traces) {
        fprintf(stdout, "-- %s \n", trace->gi_->get_type().c_str());
    }
   
    uint64_t numM = key_num_in_million;
    uint64_t num = numM * 1000000;
    KeyComparator cmp;
    Arena arena;
    SkipList<Key, KeyComparator> list(cmp, &arena);
    for (uint64_t i = 1; i <= num; i++) {
        Trace* trace = PickTrace();
        Key key(trace->Next());
        list.Insert(key);
        if(i % 1000000 == 0) fprintf(stdout, "%lluM keys inserted\n", (unsigned long long) i / 1000000);
        if(i > FLAGS_KEY_BASE_NUM && i % (1000000*FLAGS_KEY_INTERVAL) == 0) { // make sure record the pivit after insert base number of keys
            int M = i / 1000000;
            std::string filename = "data/" + WorkLoadName() + std::to_string(M) + "M.txt";
            std::string filename_pivit = "data/pivit_" + WorkLoadName() + std::to_string(M) + "M.txt";
            // SaveList(list, filename.c_str());
            SaveListPivit(list, filename_pivit.c_str(), M);
        }
    }
}

enum WorkloadMode WorkloadMode(char* buffer) {
    std::string mode(buffer);
    if("single" == mode) {
        return kSingleWorkload; // single worgit kload mode
    }
    else if("mix" == mode) {
        return kMixWorkload; // mix workload mode
    }
    return kSingleWorkload;
}
Trace* GenerateTrace(char* buffer) {
    std::string p(buffer);
    if(p == "uniform") return new TraceUniform(123);
    else if(p == "zipf") return new TraceZipfian(kYCSB_SEED);
    else if(p == "exp") return new TraceExponential(345, 50, 80);
    else if(p == "normal") return new TraceNormal(456, 0, 50000000);
    else return new TraceUniform(567);
}


int main(int argc, char** argv) {
    std::srand(std::time(nullptr)); // use current time as seed for random generator
    
    for (int i = 1; i < argc; i++) {
        int n;
        char* buffer;
        buffer = (char*)malloc(200 * sizeof(char));
        if (sscanf(argv[i], "--key=%dM", &n) == 1) {
            if(n > 0) FLAGS_KEY_NUM = n;
        } else if (sscanf(argv[i], "--pattern=%s", buffer) == 1) {
            traces.push_back(GenerateTrace(buffer));
            FLAGS_Trace = traces[0]; // gi_array[0] is used for single workload mode
        } else if (sscanf(argv[i], "--base=%dM", &n) == 1) {
            FLAGS_KEY_BASE_NUM = n * 1000000;
        } else if (sscanf(argv[i], "--interval=%dM", &n) == 1) {
            if(n > 0) FLAGS_KEY_INTERVAL = n;
        } else if (sscanf(argv[i], "--mode=%s", buffer) == 1) {
            FLAGS_WORDLOAD_MODE = WorkloadMode(buffer);
        } else {
            fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            exit(1);
        }
    }

    fprintf(stdout, "Workload mode: %s\n", FLAGS_WORDLOAD_MODE == kSingleWorkload ? "single" : "mix");
    PivitDraw(FLAGS_KEY_NUM, FLAGS_WORDLOAD_MODE);
   
    return 0;
}

// ./test_trace_test --key=150  --base=0 --interval=1 --mode=mix --pattern=zipf