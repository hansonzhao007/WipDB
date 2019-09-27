#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdint.h>
#include <assert.h>
#include <inttypes.h>

#include "util/hpblock.h"
#include "util/mutexlock.h"

namespace kv {

// setting huge page size 2MB
#define HUGEPAGE_SIZE (2UL*1024*1024)

// #define LENGTH (256UL*1024*1024)
#define PROTECTION (PROT_READ | PROT_WRITE)

#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000 /* arch specific */
#endif

/* Only ia64 requires this */
#ifdef __ia64__
#define ADDR (void *)(0x8000000000000000UL)
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_FIXED)
#else
#define ADDR (void *)(0x0UL)
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB)
#endif

const size_t kOccupySlot = 3;


HugePageBlock::HugePageBlock(int size) {
    // length should be several times of 4KB
    assert(size % 4096 == 0);
    size_ = (size / HUGEPAGE_SIZE) * HUGEPAGE_SIZE;
    if (size_ < size) {
        size_ += HUGEPAGE_SIZE;
    }
    initial_addr_ = (char*) mmap(ADDR, size_, PROTECTION, FLAGS, -1, 0);
    if (initial_addr_ == MAP_FAILED) {
        fprintf(stderr, "mmap %d hugepage fail.\n", (int)size_);
        exit(1);
    }

    // how many bitmap
    //         # of 4K       32 4K per bitmap
    length_ = (size_ >> 12) / 32;
    assert(length_ > 0);
    // create bitmaps and free all space
    bitmaps_ = new uint32_t[length_];
    for (int i = 0; i < length_; ++i) {
        bitmaps_[i] = 0xffffffff;
    }
    
}

HugePageBlock::~HugePageBlock(){
    /* munmap() size_ of MAP_HUGETLB memory must be hugepage aligned */
	if (munmap(initial_addr_, size_)) {
        fprintf(stderr, "munmap %d hugepage fail.\n", (int)size_);
		exit(1);
	}
    delete []bitmaps_;
}

void HugePageBlock::SetBitmap(int i) {
    // set bit i to 0
    assert(i < length_ * 32);
    int mask = 1;
    bitmaps_[i >> 5] &= (~ (mask << (i & 0xff))); 
}

void HugePageBlock::ResetBitmap(int i) {
    // set i to 1
    assert(i < length_ * 32);
    int mask = 1;
    bitmaps_[i >> 5] |= (mask << (i & 0xff)); 
}

char* HugePageBlock::GetBlock() {
    // get a 4KB block
    int i = FindFreeBlockIndex();
    if(i == length_ << 5) {
        // no free space
        return nullptr;
    }
    SetBitmap(i);
    return initial_addr_ + (i << 12);
}


bool HugePageBlock::ReleaseBlock(char* addr) {
    uint64_t offset = addr - initial_addr_;
    int index = offset >> 12;
    ResetBitmap(index);
    return true;
}

int HugePageBlock::FindFreeBlockIndex() {
    int i = 0, offset = 1;
    for (; i < length_; i++) {
        if (bitmaps_[i] == 0x00000000) continue;
        offset = __builtin_ffs(bitmaps_[i]);
        break;
    }
    return i * 32 + offset - 1;
}

}
