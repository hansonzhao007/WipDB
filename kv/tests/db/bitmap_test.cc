#include "db/bitmap.h"

#include "util/color.h"
#include "gtest/gtest.h"
#include <string>
#include <iostream>

using namespace kv;

TEST(BITMAP, SET_GET) {
    BitMaps bitmaps;
    bitmaps.SetSlotAtLevel(6, 7);
    uint8_t res = 1 << 7;
    ASSERT_EQ(bitmaps.BitMapAtLevel(6), res);

    // in little endian system
    bitmaps.set_bitmaps(0x1122334455667788);
    ASSERT_EQ(bitmaps.BitMapAtLevel(0), 0x88);
    ASSERT_EQ(bitmaps.BitMapAtLevel(1), 0x77);
    ASSERT_EQ(bitmaps.BitMapAtLevel(2), 0x66);
    ASSERT_EQ(bitmaps.BitMapAtLevel(3), 0x55);
    ASSERT_EQ(bitmaps.BitMapAtLevel(4), 0x44);
    ASSERT_EQ(bitmaps.BitMapAtLevel(5), 0x33);
    ASSERT_EQ(bitmaps.BitMapAtLevel(6), 0x22);

}

TEST(BITMAP, RESET) {
    BitMaps bitmaps;
    bitmaps.SetSlotAtLevel(6, 7);
    bitmaps.ResetLevel(6);
    ASSERT_EQ(bitmaps.BitMapAtLevel(6), 0);
}

TEST(BITMAP, Next) {
    BitMaps bitmaps;
    for(int i = 0; i < 8; i++) {
        GTEST_COUT << bitmaps.NextSlotAtLevel(1) << std::endl;
        bitmaps.SetSlotAtLevel(1, bitmaps.NextSlotAtLevel(1));
    }
    ASSERT_EQ(bitmaps.NextSlotAtLevel(1), -1);
    ASSERT_EQ(0xff, bitmaps.BitMapAtLevel(1));
}

TEST(BITMAP, Operator) {
    BitMaps bitmaps;
    bitmaps.set_bitmaps(0x1122334455667788);
    bitmaps[0] = 0x99;
    ASSERT_EQ(bitmaps.bitmaps(), 0x1122334455667799);
}

int main(int argc, char **argv) {
   // ::testing::internal::CaptureStdout();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}