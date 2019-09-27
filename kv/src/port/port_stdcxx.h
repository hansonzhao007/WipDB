// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_PORT_PORT_STDCXX_H_
#define STORAGE_LEVELDB_PORT_PORT_STDCXX_H_

#include "port/port_config.h"

#if HAVE_CRC32C
#include <crc32c/crc32c.h>
#endif  // HAVE_CRC32C

#if HAVE_SNAPPY
#include <snappy.h>
#endif  // HAVE_SNAPPY

#include <stddef.h>
#include <stdint.h>
#include <cassert>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <string>
#include "port/atomic_pointer.h"
#include "port/thread_annotations.h"

namespace kv {
namespace port {

static const bool kLittleEndian = !LEVELDB_IS_BIG_ENDIAN;

class CondVar;

// Thinly wraps std::mutex.
class LOCKABLE Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;

  void Lock() EXCLUSIVE_LOCK_FUNCTION() { mu_.lock(); }
  void Unlock() UNLOCK_FUNCTION() { mu_.unlock(); }
  void AssertHeld() ASSERT_EXCLUSIVE_LOCK() { }

 private:
  friend class CondVar;
  std::mutex mu_;
};

// Thinly wraps std::condition_variable.
class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(mu) { assert(mu != nullptr); }
  ~CondVar() = default;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  // 在使用 CondVar 的 Wait 之前，必须确保 mu 已经被当前 thread 获得。
  void Wait() {
    // std::adopt_lock: Assumes the calling thread already owns mu_->mu_.
    std::unique_lock<std::mutex> lock(mu_->mu_, std::adopt_lock);
    // Start waiting for the Condition Variable to get signaled
    // Wait() will internally release the lock and make the thread to block
    // As soon as condition variable get signaled, resume the thread and
    // again acquire the lock
    cv_.wait(lock);

    // 之所以要 release 是为了去除 lock 对 mu_->mu_ 的 ownership，
    // 这样在 lock 执行析构函数时候，就不会 unlock mu_->mu_。因为已经无关了。
    // 看 unique_lock 的析构函数解释是：unlocks the associated mutex, if owned 
    lock.release();// disassociates the associated mutex without unlocking it. 
  }
  void Signal() { cv_.notify_one(); }
  void SignalAll() { cv_.notify_all(); }
 private:
  std::condition_variable cv_;
  Mutex* const mu_;
};

inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {
#if HAVE_SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif  // HAVE_SNAPPY

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#if HAVE_SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif  // HAVE_SNAPPY
}

inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#if HAVE_SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif  // HAVE_SNAPPY
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  return false;
}

inline uint32_t AcceleratedCRC32C(uint32_t crc, const char* buf, size_t size) {
#if HAVE_CRC32C
  return ::crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(buf), size);
#else
  return 0;
#endif  // HAVE_CRC32C
}

}  // namespace port
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_STDCXX_H_
