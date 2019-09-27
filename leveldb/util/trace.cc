#include "util/trace.h"
#include <cmath>
#include <cassert>
#include <algorithm>

namespace leveldb {

int ShuffleA(int i) {
  static Trace* trace = new TraceUniform(142857);
  return trace->Next() % i;
}

int ShuffleB(int i) {
  static Trace* trace = new TraceUniform(285714);
  return trace->Next() % i;
}

int ShuffleC(int i) {
  static Trace* trace = new TraceUniform(428571);
  return trace->Next() % i;
}

int ShuffleD(int i) {
  static Trace* trace = new TraceUniform(571428);
  return trace->Next() % i;
}

int ShuffleE(int i) {
  static Trace* trace = new TraceUniform(714285);
  return trace->Next() % i;
}

int ShuffleF(int i) {
  static Trace* trace = new TraceUniform(857142);
  return trace->Next() % i;
}

static TraceUniform sequence_shuffle(12345678);
int ShuffleSeq(int i) {
    return sequence_shuffle.Next() % i;
}

void RandomSequence(uint64_t num, std::vector<uint64_t>& sequence){
    sequence.resize(num);
    for (uint64_t i = 0; i < num; ++i) {
      sequence[i] = i;
    }
    sequence_shuffle.Reset(); // make sure everytime we generate the same random sequence
    std::random_shuffle(sequence.begin(), sequence.end(), ShuffleSeq);
}

// uniform
TraceUniform::TraceUniform(int seed, uint64_t minimum, uint64_t maximum):
    Trace(seed) {
    gi_ = new GenInfo();
    gi_->gen.uniform.min = minimum;
    gi_->gen.uniform.max = maximum;
    gi_->gen.uniform.interval = (double)(maximum - minimum);
    gi_->type = GEN_UNIFORM;
}

uint64_t TraceUniform::Next() {
    const uint64_t off = (uint64_t)(RandomDouble() * gi_->gen.uniform.interval);
    return gi_->gen.uniform.min + off;
}

// zipfian
TraceZipfian::TraceZipfian(int seed, uint64_t minimum, uint64_t maximum):
    Trace(seed), range_(maximum) {
    gi_ = new GenInfo();
    struct GenInfo_Zipfian * const gz = &(gi_->gen.zipfian);
    
    const uint64_t items = maximum - minimum + 1;
    gz->nr_items = items;
    gz->base = minimum;
    gz->zipfian_constant = ZIPFIAN_CONSTANT;
    gz->theta = ZIPFIAN_CONSTANT;
    gz->zeta2theta = Zeta(2, ZIPFIAN_CONSTANT);
    gz->alpha = 1.0 / (1.0 - ZIPFIAN_CONSTANT);
    double zetan = Zeta(items, ZIPFIAN_CONSTANT);
    gz->zetan = zetan;
    gz->eta = (1.0 - std::pow(2.0 / (double)items, 1.0 - ZIPFIAN_CONSTANT)) / (1.0 - (gz->zeta2theta / zetan));
    gz->countforzeta = items;
    gz->min = minimum;
    gz->max = maximum;

    gi_->type = GEN_ZIPFIAN;
}

double TraceZipfian::Zeta(const uint64_t n, const double theta) {
    // assert(theta == zetalist_theta);
    const uint64_t zlid0 = n / zetalist_step;
    const uint64_t zlid = (zlid0 > zetalist_count) ? zetalist_count : zlid0;
    const double sum0 = zetalist_double[zlid];
    const uint64_t start = zlid * zetalist_step;
    const uint64_t count = n - start;
    assert(n > start);
    const double sum1 = ZetaRange(start, count, theta);
    return sum0 + sum1;
}

double TraceZipfian::ZetaRange(const uint64_t start, const uint64_t count, const double theta) {
    double sum = 0.0;
    if (count > 0x10000000) {
        fprintf(stderr, "zeta_range would take a long time... kill me or wait\n");
    }
    for (uint64_t i = 0; i < count; i++) {
        sum += (1.0 / pow((double)(start + i + 1), theta));
    }
    return sum;
}

uint64_t TraceZipfian::FNVHash64(const uint64_t value) {
    uint64_t hashval = FNV_OFFSET_BASIS_64;
    uint64_t val = value;
    for (int i = 0; i < 8; i++)
    {
        const uint64_t octet=val & 0x00ff;
        val = val >> 8;
        // FNV-1a
        hashval = (hashval ^ octet) * FNV_PRIME_64;
    }
    return hashval;
}

uint64_t TraceZipfian::NextRaw() {
// simplified: no increamental update
    const GenInfo_Zipfian *gz = &(gi_->gen.zipfian);
    const double u = RandomDouble();
    const double uz = u * gz->zetan;
    if (uz < 1.0) {
        return gz->base + 0lu;
    } else if (uz < (1.0 + pow(0.5, gz->theta))) {
        return gz->base + 1lu;
    }
    const double x = ((double)gz->nr_items) * pow(gz->eta * (u - 1.0) + 1.0, gz->alpha);
    const uint64_t ret = gz->base + (uint64_t)x;
    return ret;
}

uint64_t TraceZipfian::Next() {
    // ScrambledZipfian. scatters the "popular" items across the itemspace.
    const uint64_t z = NextRaw();
    const uint64_t xz = gi_->gen.zipfian.min + (FNVHash64(z) % gi_->gen.zipfian.nr_items);
    return xz % range_;
}

// exponential
TraceExponential::TraceExponential(int seed, const double percentile, double range):
    Trace(seed), range_(range) {
    range = range * 0.15;
    gi_ = new GenInfo();
    gi_->gen.exponential.gamma = - log(1.0 - (percentile/100.0)) / range;

    gi_->type = GEN_EXPONENTIAL;
}

uint64_t TraceExponential::Next() {
    uint64_t d = (uint64_t)(- log(RandomDouble()) / gi_->gen.exponential.gamma) % range_;
    return d;
}

// ===================================================
// = Normal Distribution Reference                   =
// = https://www.johndcook.com/blog/cpp_phi_inverse/ =
// ===================================================
TraceNormal::TraceNormal(int seed, uint64_t minimum, uint64_t maximum):
    Trace(seed) {
    gi_ = new GenInfo();
    gi_->gen.normal.min = minimum;
    gi_->gen.normal.max = maximum;
    gi_->gen.normal.mean = (maximum + minimum) / 2;
    gi_->gen.normal.stddev = (maximum - minimum) / 4;
    gi_->type = GEN_NORMAL;
}

uint64_t TraceNormal::Next() {
    double p;
    double val = 0;
    double random = 0;
    do {
      p = RandomDouble();
      if (p < 0.8)
      {
          // F^-1(p) = - G^-1(p)
          double t = sqrt(-2.0 * log(p));
          random = -(t - ((0.010328 * t + 0.802853) * t + 2.515517) / 
                (((0.001308 * t + 0.189269) * t + 1.432788) * t + 1.0));
      }
      else {
        double t = sqrt(-2.0 * log(1 - p));
        random = t - ((0.010328 * t + 0.802853)*t + 2.515517) / 
                (((0.001308 * t + 0.189269) * t + 1.432788) * t + 1.0);
      }
      val = (random + 5) * (gi_->gen.normal.max - gi_->gen.normal.min) / 10;
    }while(val < gi_->gen.normal.min || val > gi_->gen.normal.max);

    return  val;
}

// using given distribution trace to generate ycsb workload
// range:   key range
// max_num: number of operations
std::vector<YCSB_Op> YCSB_LoadGenerate(int64_t range, uint64_t max_num, YCSBLoadType type, Trace* trace, const std::vector<uint64_t>& ycsb_insertion_sequence) {
    std::vector<YCSB_Op> res;
    switch (type) {
        case kYCSB_A: {
            // 50% reads
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_Read;
                res.push_back(ops);
            }
            // 50% updates(writes)
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleA);
            break;
        }
        
        case kYCSB_B: {
            // B: 95% reads, 5% writes
            uint64_t R95 = max_num * 0.95;
            uint64_t W5  = max_num - R95;
            for (uint64_t i = 0; i < R95; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Read;
                res.push_back(ops);
            }
            for (uint64_t i = 0; i < W5; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleB);
            break;
        }
        

        case kYCSB_C: {
            // 100% reads
            for (uint64_t i = 0; i < max_num; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Read;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleC);
            break;
        }

        case kYCSB_D: {
            // D: operate on latest inserted records
            uint64_t R95 = max_num * 0.95;
            uint64_t W5  = max_num - R95;
            for (uint64_t i = 0; i < R95; ++i) {
                YCSB_Op ops;
                ops.key = ycsb_insertion_sequence[trace->Next() % range];
                ops.type= kYCSB_Read;
                res.push_back(ops);
            }
            for (uint64_t i = 0; i < W5; ++i) {
                YCSB_Op ops;
                ops.key = ycsb_insertion_sequence[trace->Next() % range];
                ops.type= kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleD);
            break;
        }
       
        case kYCSB_E: {
            // 95% range queries, 5% writes
            // 
            uint64_t R95 = max_num * 0.95;
            uint64_t W5  = max_num - R95;
            for (uint64_t i = 0; i < R95; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Query;
                // number of scan is uniform distribution
                res.push_back(ops);
            }
            for (uint64_t i = 0; i < W5; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleE);
            break;
        }
        

        case kYCSB_F: {
            // 50% reads
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_Read;
                res.push_back(ops);
            }
            // 50% read-modified-writes
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_ReadModifyWrite;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleF);
            break;
        }

        default: {
            perror("Not a valid ycsb workload type.\n");
        }
    }
    if (res.size() != max_num) {
        perror("YCSB workload size incorrect\n");
        abort();
    }
    return res;
}

}
