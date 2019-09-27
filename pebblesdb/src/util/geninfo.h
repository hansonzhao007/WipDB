#ifndef KV_GENINFO_H_
#define KV_GENINFO_H_

#include <stdint.h>
#include <string>

namespace leveldb {

#ifndef typeof
#define typeof __typeof__
#endif

enum GeneratorType {
  GEN_CONSTANT = 0, // always a constant number
  GEN_COUNTER,      // +1 each fetch
  GEN_DISCRETE,     // gen within a set of values with its weight as probability
  GEN_EXPONENTIAL,  // exponential
  GEN_FILE,         // string from lines in file
  GEN_HISTOGRAM,    // histogram
  GEN_HOTSET,       // hot/cold set
  GEN_ZIPFIAN,      // Zipfian, 0 is the most popular.
  GEN_XZIPFIAN,     // ScrambledZipfian. scatters the "popular" items across the itemspace.
  GEN_LATEST,       // Generate a popularity distribution of items, skewed to favor recent items.
  GEN_UNIFORM,      // Uniformly distributed in an interval [a,b]
  GEN_NORMAL,       // Normal distribution
};

struct GenInfo_Constant {
  uint64_t constant;
};

struct GenInfo_Counter {
  uint64_t counter;
};

struct Pair64 {
  uint64_t a;
  uint64_t b;
};

struct GenInfo_Discrete {
  uint64_t nr_values;
  struct Pair64 *pairs;
};

struct GenInfo_Exponential {
  double gamma;
  uint64_t max;
};

struct GenInfo_File {
  FILE * fin;
};

struct GenInfo_Histogram {
  uint64_t block_size;
  uint64_t area;
  uint64_t weighted_area;
  double main_size;
  uint64_t *buckets;
};

struct GenInfo_HotSet {
  uint64_t lower_bound;
  uint64_t upper_bound;
  uint64_t hot_interval;
  uint64_t cold_interval;
  double hot_set_fraction;
  double hot_op_fraction;
};

#define ZIPFIAN_CONSTANT ((0.85))
struct GenInfo_Zipfian {
  uint64_t nr_items;
  uint64_t base;
  double zipfian_constant;
  double theta;
  double zeta2theta;
  double alpha;
  double zetan;
  double eta;
  uint64_t countforzeta;
  uint64_t min;
  uint64_t max;
};

struct GenInfo_Latest {
  struct GenInfo_Zipfian zipfion;
  uint64_t max;
};

struct GenInfo_Uniform {
  uint64_t min;
  uint64_t max;
  double interval;
};

struct GenInfo_Normal {
  uint64_t mean;
  uint64_t stddev;
  uint64_t min;
  uint64_t max;
};

struct GenInfo {
  uint64_t (*next)(struct GenInfo * const);
  enum GeneratorType type;
  union {
    struct GenInfo_Constant     constant;
    struct GenInfo_Counter      counter;
    struct GenInfo_Discrete     discrete;
    struct GenInfo_Exponential  exponential;
    struct GenInfo_File         file;
    struct GenInfo_Histogram    histogram;
    struct GenInfo_HotSet       hotset;
    struct GenInfo_Zipfian      zipfian;
    struct GenInfo_Latest       latest;
    struct GenInfo_Uniform      uniform;
    struct GenInfo_Normal       normal;
  } gen;
  std::string get_type();
};
}
#endif