#include "db/bucket.h"
#include "util/trace.h"
#include "kv/env.h"
#include "gflags/gflags.h"

using namespace std;
using namespace kv;

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_int64(num, 1000, "Number of buckets");

int main(int argc, char** argv) {
    ParseCommandLineFlags(&argc, &argv, true);
    Trace* trace = new TraceUniform(1000);
    std::vector<Bucket*> buckets;
    for (int i = 0; i < FLAGS_num; ++i ) {
        Bucket* node = new Bucket();
        char msg[100];
        snprintf(msg, sizeof(msg), "%016d", trace->Next() % 100000000);
        node->largest = msg;
        buckets.push_back(node);
    }
    sort(buckets.begin(), buckets.end(), BucketCmp());

    auto start = Env::Default()->NowMicros();
    for (int i = 0; i < 100000; i++) {
        char key[100];
        snprintf(key, sizeof(key), "%016d", trace->Next() % 100000000);
        int l = Bucket::lower_bound(buckets, key);
    }
    auto end = Env::Default()->NowMicros();
    printf("Average search time: %f\n", (end - start)/ 100000.0);
    return 0;
}
