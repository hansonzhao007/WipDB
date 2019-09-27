#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <vector>

#include "util/trace.h"
#include "kv/options.h"
#include "kv/env.h"


#define LENGTH (256UL*1024*1024)

using namespace kv;

int main(void)
{
	// int shmid;
	unsigned long i;

	Trace * trace = new TraceUniform(123);
	
    std::vector<char*> blocks;
    for (int i = 0; i < 65536; ++i) {
        blocks.push_back(new char[4096]);
    }

	printf("Starting random write\n");
	uint64_t start_huge = Env::Default()->NowMicros();
	for (i = 0; i < LENGTH; i++) {
		int index = trace->Next() % LENGTH;
		blocks[index >> 12][index & 0xfff] = (char)(i);

	}
	uint64_t end_huge = Env::Default()->NowMicros();
	fprintf(stdout, "Running time: %llu\n", (unsigned long long) end_huge - start_huge);

	return 0;
}

// perf stat -e dTLB-load-misses,iTLB-load-misses ./tests/util/test_newspace_random_test
// Starting random write
// Running time: 9618744

//  Performance counter stats for './tests/util/test_newspace_random_test':

//            358,479      dTLB-load-misses
//            136,267      iTLB-load-misses

//        9.741774723 seconds time elapsed

//        9.416755000 seconds user
//        0.324163000 seconds sys

