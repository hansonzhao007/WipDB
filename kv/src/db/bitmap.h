
#ifndef KV_DB_BITMAP_H_
#define KV_DB_BITMAP_H_

#include "stdint.h"
#include "db/dbformat.h"

namespace kv {

class BitMaps {
public:
    // index:  7 6 5 4 3 2 1 0
    // bitmap: 1 1 0 0 0 0 0 0 
    // __builtin_ffs(bitmap) == 7
    // __builtin_ffs(   0  ) == 0

    int NextSlotAtLevel(int level) const {
        // slot 按照从高到低的顺序占用。
        // 如果 slot 满了，return -1
        assert(level >= 0 && level < config::kNumLevels);
        int first0_from_left = __builtin_ffs(bitmaps_.at[level]) - 2;
        if(first0_from_left == -2) return 7;
        return first0_from_left;
    }
    void SetSlotAtLevel(int level, int i) {
        assert(level >= 0 && level < config::kNumLevels);
        assert(i >=0 && i < config::kKV_Level_Ratio);
        uint8_t pos = 1 << i;
        assert(!(bitmaps_.at[level] & pos));
        bitmaps_.at[level] |= pos;
    }

    void ResetLevel(int level) {
        assert(level >= 0 && level < config::kNumLevels);
        bitmaps_.at[level] = 0;
    }

    uint8_t BitMapAtLevel(int level) {
        assert(level >= 0 && level < config::kNumLevels);
        return bitmaps_.at[level];
    }

    uint64_t bitmaps() const {
        return bitmaps_.all;
    }

    void set_bitmaps(uint64_t bm) {
        bitmaps_.all = bm;
    }

    BitMaps() {
        bitmaps_.all  = 0;
    }

    BitMaps(uint64_t bm) {
        bitmaps_.all  = bm;
    }

    uint8_t& operator[](int level) { return bitmaps_.at[level];}
private:
    union {
        uint8_t at[config::kNumLevels];
        uint64_t all;
    }bitmaps_;

};

}


#endif