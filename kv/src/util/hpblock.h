#ifndef KV_HUGEPAGE_BLOCK_H
#define KV_HUGEPAGE_BLOCK_H

#include <stddef.h>
#include <vector>
#include "port/port.h"
#include "port/thread_annotations.h"

namespace kv {

class HugePageBlock {
public:
    // support multithread
    char* GetBlock();
    bool ReleaseBlock(char* addr );
    
    // create a hugepage block from offset, total space size
    HugePageBlock(int size);

    ~HugePageBlock();
    char* GetAddr() {return initial_addr_;}
private:

    int FindFreeBlockIndex();

    void SetBitmap(int i);
    void ResetBitmap(int i);

    char* initial_addr_;
    size_t size_; // space size

    // every bitmap mapping 32 4KB space, one bitmap can manage 128KB space
    uint32_t * bitmaps_;
    int length_; // how many bitmap, every bitmap map 32 4K space, aka 128KB
    
};

}

#endif