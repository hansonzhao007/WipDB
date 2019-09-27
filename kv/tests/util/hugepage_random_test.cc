// SPDX-License-Identifier: GPL-2.0
/*
 * Example of using hugepage memory in a user application using the mmap
 * system call with MAP_HUGETLB flag.  Before running this program make
 * sure the administrator has allocated enough default sized huge pages
 * to cover the 256 MB allocation.
 *
 * For ia64 architecture, Linux kernel reserves Region number 4 for hugepages.
 * That means the addresses starting with 0x800000... will need to be
 * specified.  Specifying a fixed address is not required on ppc64, i386
 * or x86_64.
 */
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <vector>

#include "util/trace.h"
#include "kv/options.h"
#include "kv/env.h"
#include "util/hpblock.h"

using namespace kv;

#define LENGTH (2048 * 1024)

int main(void)
{
	// char *addr;
	// int ret;
	Trace* trace = new TraceUniform(134);

	HugePageBlock page_block(4096); // allocate 2MB huge page, if size < 2MB, it will allocate 2MB in default
	std::vector<char*> blocks;
	
	int j = 0;
	// 512 4KB blocks
	for (; j < 512; ++j) {
		char* addr = page_block.GetBlock();
		assert(addr != nullptr);
		blocks.push_back(addr);
	}
	char* addr = page_block.GetBlock();
	assert(addr == nullptr);
	
	printf("Block size should be 512: %d\n", (int)blocks.size());

	printf("Starting random write\n");
	uint64_t start_huge = Env::Default()->NowMicros();
	for (int i = 0; i < LENGTH; i++) {
		int index = trace->Next() % LENGTH;
		blocks[index >> 12][index & 0xfff] = (char)(i);
	}
	uint64_t end_huge = Env::Default()->NowMicros();
	fprintf(stdout, "Running time: %llu\n", (unsigned long long) end_huge - start_huge);

	return 0;
}

// /perf stat -e dTLB-load-misses,iTLB-load-misses ./tests/util/test_hugepage_random_test
// Block size should be 65536: 65536
// Starting random write
// Running time: 4015869

//  Performance counter stats for './tests/util/test_hugepage_random_test':

//              2,125      dTLB-load-misses
//              1,071      iTLB-load-misses

//        4.020703788 seconds time elapsed

//        3.976720000 seconds user
//        0.044007000 seconds sys