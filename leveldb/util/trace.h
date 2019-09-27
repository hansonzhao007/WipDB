#ifndef KV_UTIL_TRACE_H
#define KV_UTIL_TRACE_H

#include <stdint.h>
#include <vector>
#include "util/geninfo.h"

namespace leveldb {
const uint64_t kRAND64_MAX = ((((uint64_t)RAND_MAX) << 31) + ((uint64_t)RAND_MAX));
const double   kRAND64_MAX_D = ((double)(kRAND64_MAX));
const uint64_t kRANDOM_RANGE = UINT64_C(2000000000000);

const uint64_t kYCSB_SEED = 1729; 
const uint64_t kYCSB_LATEST_SEED = 1089; 

enum YCSBLoadType {kYCSB_A, kYCSB_B, kYCSB_C, kYCSB_D, kYCSB_E, kYCSB_F};
enum YCSBOpType {kYCSB_Write, kYCSB_Read, kYCSB_Query, kYCSB_ReadModifyWrite};

struct YCSB_Op {
  YCSBOpType type;
  uint64_t key;
};

class Trace {
public:

  Trace(int seed):seed_(seed), init_(seed), gi_(nullptr){}
  
  virtual ~Trace() {if(gi_ != nullptr) delete gi_;}
  virtual uint64_t Next() = 0;
  void Reset() {seed_ = init_;}
  uint32_t Random() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if ((uint32_t)seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  uint64_t Random64() {
    // 62 bit random value;
    const uint64_t rand64 = (((uint64_t)Random()) << 31) + ((uint64_t)Random());
    return rand64;
  }

  double RandomDouble() {
    // random between 0.0 - 1.0
    const double r = (double)Random64();
    const double rd = r / kRAND64_MAX_D;
    return rd;
  }

    
  int seed_;
  int init_;
  GenInfo* gi_;

};

class TraceUniform: public Trace {
public:
  explicit TraceUniform(int seed, uint64_t minimum = 0, uint64_t maximum = kRANDOM_RANGE);
  ~TraceUniform() {}
  uint64_t Next() override;
};

class TraceZipfian: public Trace {
public:
  explicit TraceZipfian(int seed, uint64_t minimum = 0, uint64_t maximum = UINT64_C(0xc0000000000));
  ~TraceZipfian() {}
  uint64_t Next() override;
  uint64_t NextRaw();
  double Zeta(const uint64_t n, const double theta);
  double ZetaRange(const uint64_t start, const uint64_t count, const double theta);
  uint64_t FNVHash64(const uint64_t value);

private:
  uint64_t zetalist_u64[17] = {0,
    UINT64_C(0x4040437dd948c1d9), UINT64_C(0x4040b8f8009bce85),
    UINT64_C(0x4040fe1121e564d6), UINT64_C(0x40412f435698cdf5),
    UINT64_C(0x404155852507a510), UINT64_C(0x404174d7818477a7),
    UINT64_C(0x40418f5e593bd5a9), UINT64_C(0x4041a6614fb930fd),
    UINT64_C(0x4041bab40ad5ec98), UINT64_C(0x4041cce73d363e24),
    UINT64_C(0x4041dd6239ebabc3), UINT64_C(0x4041ec715f5c47be),
    UINT64_C(0x4041fa4eba083897), UINT64_C(0x4042072772fe12bd),
    UINT64_C(0x4042131f5e380b72), UINT64_C(0x40421e53630da013),
  };

  double* zetalist_double = (double*)zetalist_u64;
  uint64_t zetalist_step = UINT64_C(0x10000000000);
  uint64_t zetalist_count = 16;
  // double zetalist_theta = 0.99;
  uint64_t range_;
};


class TraceExponential: public Trace {
public:
  #define FNV_OFFSET_BASIS_64 ((UINT64_C(0xCBF29CE484222325)))
  #define FNV_PRIME_64 ((UINT64_C(1099511628211)))
  explicit TraceExponential(int seed, const double percentile = 50, double range = kRANDOM_RANGE);
  ~TraceExponential()  {}
  uint64_t Next() override;
private:
  uint64_t range_;
};


class TraceNormal: public Trace {
public:
  explicit TraceNormal(int seed, uint64_t minimum = 0, uint64_t maximum = kRANDOM_RANGE);
  ~TraceNormal() {}
  uint64_t Next() override;
};

void RandomSequence(uint64_t num, std::vector<uint64_t>& sequence);
// generate ycsb workload. For workload D, we need insertion order sequence: ycsb_insertion_sequence
std::vector<YCSB_Op> YCSB_LoadGenerate(int64_t range, uint64_t max_num, YCSBLoadType type, Trace* selector, const std::vector<uint64_t>& ycsb_insertion_sequence);
  

}

#endif
