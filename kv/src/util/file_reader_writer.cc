//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/file_reader_writer.h"

#include <algorithm>
#include <mutex>

#include "monitoring/histogram.h"
#include "port/port.h"
#include "util/random.h"
#include "util/rate_limiter.h"

namespace kv {

#ifndef NDEBUG
namespace {
bool IsFileSectorAligned(const size_t off, size_t sector_size) {
  return off % sector_size == 0;
}
}
#endif

Status WritableFileWriter::Append(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();
  Status s;
  pending_sync_ = true;

  {
    writable_file_->PrepareWrite(static_cast<size_t>(GetFileSize()), left);
  }
  // See whether we need to enlarge the buffer to avoid the flush
  if (buf_.Capacity() - buf_.CurrentSize() < left) {
    for (size_t cap = buf_.Capacity();
         cap < max_buffer_size_;  // There is still room to increase
         cap *= 2) {
      // See whether the next available size is large enough.
      // Buffer will never be increased to more than max_buffer_size_.
      size_t desired_capacity = std::min(cap * 2, max_buffer_size_);
      if (desired_capacity - buf_.CurrentSize() >= left ||
          (use_direct_io() && desired_capacity == max_buffer_size_)) {
        buf_.AllocateNewBuffer(desired_capacity, true);
        // printf("Allocate: %dkb. Capacity: %d\n", (int)desired_capacity/1024, buf_.Capacity());
        break;
      }
    }
  }

  // Flush only when buffered I/O
  if (!use_direct_io() && (buf_.Capacity() - buf_.CurrentSize()) < left) {
    if (buf_.CurrentSize() > 0) {
      // printf("Flush buf: %dkb\n", (int)buf_.CurrentSize()/1024);
      s = Flush();
      if (!s.ok()) {
        return s;
      }
    }
    assert(buf_.CurrentSize() == 0);
  }

  // We never write directly to disk with direct I/O on.
  // or we simply use it for its original purpose to accumulate many small
  // chunks
  if (use_direct_io() || (buf_.Capacity() >= left)) {
    while (left > 0) {
      size_t appended = buf_.Append(src, left);
      left -= appended;
      src += appended;

      if (left > 0) {
        // printf("Flush size: %d. Capacity: %d\n", buf_.CurrentSize(), buf_.Capacity());
        // 已经 buffer 满了一个对齐的block
        s = Flush();
        if (!s.ok()) {
          break;
        }
      }
    }
  } else {
    // Writing directly to file bypassing the buffer
    // printf("Bypassing buffer\n");
    assert(buf_.CurrentSize() == 0);
    s = WriteBuffered(src, left);
  }

  
  if (s.ok()) {
    filesize_ += data.size();
  }
  return s;
}


Status WritableFileWriter::Pad(const size_t pad_bytes) {
  assert(pad_bytes < kDefaultPageSize);
  size_t left = pad_bytes;
  size_t cap = buf_.Capacity() - buf_.CurrentSize();

  // Assume pad_bytes is small compared to buf_ capacity. So we always
  // use buf_ rather than write directly to file in certain cases like
  // Append() does.
  while (left) {
    size_t append_bytes = std::min(cap, left);
    buf_.PadWith(append_bytes, 0);
    left -= append_bytes;
    if (left > 0) {
      Status s = Flush();
      if (!s.ok()) {
        return s;
      }
    }
    cap = buf_.Capacity() - buf_.CurrentSize();
  }
  pending_sync_ = true;
  filesize_ += pad_bytes;
  return Status::OK();
}

Status WritableFileWriter::Close() {

  // Do not quit immediately on failure the file MUST be closed
  Status s;

  // Possible to close it twice now as we MUST close
  // in __dtor, simply flushing is not enough
  // Windows when pre-allocating does not fill with zeros
  // also with unbuffered access we also set the end of data.
  if (!writable_file_) {
    return s;
  }

  s = Flush();  // flush cache to OS

  Status interim;
  // In direct I/O mode we write whole pages so
  // we need to let the file know where data ends.
  
  if (use_direct_io()) {
    interim = writable_file_->Truncate(filesize_);
    if (interim.ok()) {
      interim = writable_file_->Fsync();
    }
    if (!interim.ok() && s.ok()) {
      s = interim;
    }
  }

  { 
    interim = writable_file_->Close();
    if (!interim.ok() && s.ok()) {
      s = interim;
    }
  }

  writable_file_ = nullptr;


  return s;
}

// write out the cached data to the OS cache or storage if direct I/O
// enabled
Status WritableFileWriter::Flush() {
  Status s;
  
  // printf("Flush size: %d\n", buf_.CurrentSize());

  // 将在 align buffer 里的内容写到文件
  if (buf_.CurrentSize() > 0) {
    
    if (use_direct_io()) {
      // printf("Direct Flush size: %d\n", buf_.CurrentSize());
      s = WriteDirect();
    } else {
      s = WriteBuffered(buf_.BufferStart(), buf_.CurrentSize());
    }
    if (!s.ok()) {
      return s;
    }
  }

  s = writable_file_->Flush();

  if (!s.ok()) {
    return s;
  }

  // sync OS cache to disk for every bytes_per_sync_
  // TODO: give log file and sst file different options (log
  // files could be potentially cached in OS for their whole
  // life time, thus we might not want to flush at all).

  // We try to avoid sync to the last 1MB of data. For two reasons:
  // (1) avoid rewrite the same page that is modified later.
  // (2) for older version of OS, write can block while writing out
  //     the page.
  // Xfs does neighbor page flushing outside of the specified ranges. We
  // need to make sure sync range is far from the write offset.
  if (!use_direct_io() && bytes_per_sync_) {
    const uint64_t kBytesNotSyncRange = 1024 * 1024;  // recent 1MB is not synced.
    const uint64_t kBytesAlignWhenSync = 4 * 1024;    // Align 4KB.
    if (filesize_ > kBytesNotSyncRange) {
      uint64_t offset_sync_to = filesize_ - kBytesNotSyncRange;
      offset_sync_to -= offset_sync_to % kBytesAlignWhenSync;
      assert(offset_sync_to >= last_sync_size_);
      if (offset_sync_to > 0 &&
          offset_sync_to - last_sync_size_ >= bytes_per_sync_) {
        s = RangeSync(last_sync_size_, offset_sync_to - last_sync_size_);
        last_sync_size_ = offset_sync_to;
      }
    }
  }

  return s;
}

Status WritableFileWriter::Sync(bool use_fsync) {
  // printf("Sync: buffer remain: %d. Capacity: %d\n", buf_.CurrentSize(), buf_.Capacity());
  Status s = Flush();
  // printf("After Flush: buffer remain: %d. Capacity: %d\n", buf_.CurrentSize(), buf_.Capacity());
  if (!s.ok()) {
    return s;
  }
  
  if (!use_direct_io() && pending_sync_) {
    s = SyncInternal(use_fsync);
    if (!s.ok()) {
      return s;
    }
  }
  
  pending_sync_ = false;
  return Status::OK();
}

Status WritableFileWriter::SyncWithoutFlush(bool use_fsync) {
  if (!writable_file_->IsSyncThreadSafe()) {
    return Status::NotSupported(
      "Can't WritableFileWriter::SyncWithoutFlush() because "
      "WritableFile::IsSyncThreadSafe() is false");
  }
  
  Status s = SyncInternal(use_fsync);
  
  return s;
}

Status WritableFileWriter::SyncInternal(bool use_fsync) {
  Status s;
  
  if (use_fsync) {
    s = writable_file_->Fsync();
  } else {
    s = writable_file_->Sync();
  }
  return s;
}

Status WritableFileWriter::RangeSync(uint64_t offset, uint64_t nbytes) {
  return writable_file_->RangeSync(offset, nbytes);
}

// This method writes to disk the specified data and makes use of the rate
// limiter if available
Status WritableFileWriter::WriteBuffered(const char* data, size_t size) {
  Status s;
  assert(!use_direct_io());
  const char* src = data;
  size_t left = size;

  
  while (left > 0) {
    // printf("WriteBuffered: %d\n", left);
    size_t allowed;
    if (rate_limiter_ != nullptr) {
      // printf("rate limit\n");
      allowed = rate_limiter_->RequestToken(
          left, 0 /* alignment */, writable_file_->GetIOPriority(), stats_,
          RateLimiter::OpType::kWrite);
    } else {
      allowed = left;
    }

    {
      if(!is_pwrite_) {
        s = writable_file_->Append(Slice(src, allowed));
      } 
      else {
        s = writable_file_->PositionedAppend(Slice(src, allowed), initial_offset_ + byte_written_ );
      }
      if (!s.ok()) {
        return s;
      }
    }

    left -= allowed;
    src += allowed;
    byte_written_ += allowed;
  }
  buf_.Size(0);
  return s;
}

// This flushes the accumulated data in the buffer. We pad data with zeros if
// necessary to the whole page.
// However, during automatic flushes padding would not be necessary.
// We always use RateLimiter if available. We move (Refit) any buffer bytes
// that are left over the
// whole number of pages to be written again on the next flush because we can
// only write on aligned
// offsets.
#ifndef ROCKSDB_LITE
Status WritableFileWriter::WriteDirect() {
  assert(use_direct_io());
  Status s;
  const size_t alignment = buf_.Alignment();
  assert((next_write_offset_ % alignment) == 0);

  // Calculate whole page final file advance if all writes succeed
  size_t file_advance =
    TruncateToPageBoundary(alignment, buf_.CurrentSize());

  // Calculate the leftover tail, we write it here padded with zeros BUT we
  // will write
  // it again in the future either on Close() OR when the current whole page
  // fills out
  size_t leftover_tail = buf_.CurrentSize() - file_advance;
  // Round up and pad
  buf_.PadToAlignmentWith(0);
  const char* src = buf_.BufferStart();
  uint64_t write_offset = next_write_offset_;
  size_t left = buf_.CurrentSize();

  while (left > 0) {
    // Check how much is allowed
    size_t size;
    if (rate_limiter_ != nullptr) {
      size = rate_limiter_->RequestToken(left, buf_.Alignment(),
                                         writable_file_->GetIOPriority(),
                                         stats_, RateLimiter::OpType::kWrite);
    } else {
      size = left;
    }

    // printf("Direct Append size: %d\n", size);
    {
      // direct writes must be positional
      s = writable_file_->PositionedAppend(Slice(src, size), write_offset + initial_offset_);
      if (!s.ok()) {
        buf_.Size(file_advance + leftover_tail);
        return s;
      }
    }
    left -= size;
    src += size;
    write_offset += size;
    assert((next_write_offset_ % alignment) == 0);
  }

  if (s.ok()) {
    // Move the tail to the beginning of the buffer
    // This never happens during normal Append but rather during
    // explicit call to Flush()/Sync() or Close()
    buf_.RefitTail(file_advance, leftover_tail);
    // This is where we start writing next time which may or not be
    // the actual file size on disk. They match if the buffer size
    // is a multiple of whole pages otherwise filesize_ is leftover_tail
    // behind
    next_write_offset_ += file_advance;
  }
  return s;
}
#endif  // !ROCKSDB_LITE

void WritableFileWriter::SetBytesPerSecond(int64_t bytes_per_second) {
  assert(bytes_per_second > 0);
  if (rate_limiter_ == nullptr) printf("nullptr limiter\n");
  else rate_limiter_->SetBytesPerSecond(bytes_per_second);
}
}  // namespace rocksdb
