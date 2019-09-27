/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#ifndef KV_UTIL_GENERATOR_H_
#define KV_UTIL_GENERATOR_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <random>

#include "util/geninfo.h"

namespace kv {

uint64_t random_uint64(void);
double random_double(void);

struct GenInfo * generator_new_constant(const uint64_t constant);

struct GenInfo * generator_new_counter(const uint64_t start);

struct GenInfo * generator_new_exponential(const double percentile, const double range);

struct GenInfo * generator_new_zipfian(const uint64_t min, const uint64_t max);

struct GenInfo * generator_new_xzipfian(const uint64_t min, const uint64_t max);

struct GenInfo * generator_new_uniform(const uint64_t min, const uint64_t max);

struct GenInfo * generator_new_normal(uint64_t min, uint64_t max);

void generator_destroy(struct GenInfo * const geninfo);

uint64_t FNV_hash64(const uint64_t value);

} // end of namespace kv
#endif