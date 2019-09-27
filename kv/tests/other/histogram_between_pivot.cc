#include "util/skiplist.h"
#include "gtest/gtest.h"


#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <iostream>
#include <unordered_set>
#include <set>
#include <map>
#include <fstream>
#include <sstream>
#include <iostream>

#include "kv/env.h"
#include "util/debug.h"
#include "util/random.h"
#include "util/generator.h"
#include "gflags/gflags.h"
#include "util/trace.h"

using namespace kv;


using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;


DEFINE_int64(num, 200, "Number of key. Million");
DEFINE_int64(base_num, 1000000, "interval to determin pivot");
DEFINE_int64(interval, 1, "interval in million");
DEFINE_string(patterns, "uniform", "");
DEFINE_string(pivot_path, "data/", "");

const uint64_t kKeyRange = kRANDOM_RANGE;


struct GenInfo* FLAGS_GI = generator_new_uniform(0, kKeyRange);
static uint64_t FLAGS_JUMP = 100; // we have 1M sample pivot totally, set this to use very jump pivot as final pivot. For example, 100 means 1M/100 = 10000 pivots
vector<GenInfo*> gi_array;

enum WorkloadMode { kSingleWorkload, kMixWorkload};
// 0: single workload 1: mixed workload
static enum WorkloadMode FLAGS_WORDLOAD_MODE = kMixWorkload;

struct Key {
  uint64_t val;
  uint64_t seq;
  Key(uint64_t v): val(v+1) {
      seq =  FNV_hash64(debug_time_usec());
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


// save the frequency of keys of each pivot
void SaveHistogram(std::map<uint64_t, uint64_t>& mmp, std::string filename) {
    FILE* fp = fopen (filename.c_str(), "w+");
    for(auto& m: mmp) {
        fprintf(fp, "%llu,\n", (unsigned long long)  m.second);
    }
    fclose(fp);
}

struct GenInfo* &PickGi() {
   return gi_array[rand() % gi_array.size()];
}

std::string WorkLoadName() {
    std::string workload_name;
    for(auto& gi: gi_array)
        workload_name += gi->get_type() + "_";
    return workload_name;
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
struct GenInfo* GenerateGi(Slice p) {
    if(p == Slice("uniform")) return generator_new_uniform(0, kKeyRange);
    else if(p == Slice("zipf")) return generator_new_zipfian(0, kKeyRange);
    else if(p == Slice("xzipf")) return generator_new_xzipfian(0, kKeyRange);
    else if(p == Slice("exp")) return generator_new_exponential(90, kKeyRange);
    else if(p == Slice("normal")) return generator_new_normal(0, kKeyRange);
    else return generator_new_uniform(0, kKeyRange);
}

// read from pivot file and build mmp
// file content:
// 1
// 100
// 300
// ...

void BuildPivot(std::map<uint64_t, uint64_t>& mmp) {
    std::ifstream infile(FLAGS_pivot_path);
    if(!infile) {
        std::perror("File opening failed");
        exit(1);
    }
    uint64_t pivot;
    std::string line;
    int i = 0;
    while(std::getline(infile, line)) {
        line.pop_back();
        std::istringstream iss(line);
        if(!iss.eof()) 
        {
            iss >> pivot;
            if(i % FLAGS_JUMP == 0)
                mmp[pivot] = 0;
        }
        i++;
    }
}

void Histogram() {
    std::cout << gi_array.size() << " wordloads" << std::endl;
    for(auto& g: gi_array) {
        fprintf(stdout, "-- %s \n", g->get_type().c_str());
    }
    uint64_t numM = FLAGS_num;
    uint64_t num = numM * 1000000;
    std::map<uint64_t, uint64_t> mmp;
    BuildPivot(mmp);

    for (uint64_t i = 1; i <= num; i++) {
        struct GenInfo* &gi = PickGi();
        uint64_t key(gi->next(gi) + 1);
        auto itr = mmp.lower_bound(key);
        if(itr != mmp.end()) {
            itr->second++;
        }

        if(i % 1000000 == 0) fprintf(stdout, "%lluM keys inserted\n", (unsigned long long) i / 1000000);

        if(i >= FLAGS_base_num && i % (1000000*FLAGS_interval) == 0) { // make sure record the pivit after insert base number of keys
            int M = i / 1000000;
            std::string filename = "data/histogram_" + WorkLoadName() + std::to_string(M) + "M.txt";
            std::cout << "Save file: " << filename << std::endl;
            SaveHistogram(mmp, filename.c_str());
        }
    }

}

int main(int argc, char** argv) {
    ParseCommandLineFlags(&argc, &argv, true);
    std::srand(std::time(nullptr)); // use current time as seed for random generator
    
    const char* patterns = FLAGS_patterns.c_str();
    while (patterns != nullptr) {
      const char* sep = strchr(patterns, ',');
      Slice name;
      if (sep == nullptr) {
        name = patterns;
        patterns = nullptr;
      } else {
        name = Slice(patterns, sep - patterns);
        patterns = sep + 1;
      }
      printf("%s\n", name.ToString().c_str());
      gi_array.push_back(GenerateGi(name));
    }

    Histogram();
   
    return 0;
}

// 指定一个pivot文件，然后根据该 pivot，统计每个 pivot 之间的 key 的频率
// ./pivot_fix --filepath=/home/hanson/data/pivot_raw/pivit_uniform_exponential_normal/pivit_uniform_exponential_normal_1M.txt --jump=100 --key=150 --interval=10 --pattern=uniform --pattern=exp --pattern=normal