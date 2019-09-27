
#ifndef KV_DB_BUCKET_H_
#define KV_DB_BUCKET_H_

#include <string>
#include <vector>
#include <memory>
#include "kv/db.h"
#include "kv/options.h"
#include "db/version_edit.h"
namespace kv {

enum SplitStatus {kSplitIdle, kShouldSplit, kSpliting, kSplitFinish};

class Bucket {
public:

    // ----------- fixed after creating 
    DB* db;
    Options options;
    int T;          // split ratio: 1 to T buckets
    std::string db_name;
    std::string largest;
    HugePageBlock* hugepage;

    // ----------- variables 
    std::atomic<int> refs_;
    std::atomic<uint64_t> last_flush_seq; // for recycle wal
    std::atomic<bool> splited; // once bucket split is triggeered by write, we set this to true
    // for split
    std::atomic<SplitStatus> spliting_status; // used for signal
    std::vector<std::string> split_pivots; // split pivots split_pivots.size() = T. The last pivot will be the largest in bucket
    // v1: |  A  | -------  b ------ |  c  |

    // v2: |  A  | b1 | b2 | b3 | b4 |  c  |
    //              |    |    |    |
    //           | -------  b ------ |    bucket_old

    // v3: |  A  | b1 | b2 | b3 | b4 |  c  |
    std::vector<FileMetaData> bottom_tables; 
    std::vector<Bucket*> split_buckets;    
    Bucket* bucket_old; // when old bucket is splitting, the new version's bucket will point to the old bucket

    Bucket(const Slice& key):
        db (nullptr),
        options(Options()),
        db_name(""),
        largest("0"),
        spliting_status(kSplitIdle),
        bucket_old(nullptr),
        splited(false),
        refs_(0),
        T(8),
        hugepage(nullptr) {
        largest = key.ToString();
        bucket_old = nullptr;
    }
    Bucket(): 
        db (nullptr),
        options(Options()),
        db_name(""),
        largest("0"),
        spliting_status(kSplitIdle),
        bucket_old(nullptr),
        splited(false),
        refs_(0),
        T(8),
        hugepage(nullptr) {}

    // find the node that >= key
    static int lower_bound(const std::vector<Bucket*>& bucket_, const  Slice& key) {
        int l = 0, r = bucket_.size() - 1;
        while(l < r) {
            int m = (l + r) >> 1;
            int res = Slice(bucket_[m]->largest).compare(key);
            if(res < 0)
                l = m + 1;
            else if(res > 0)
                r = m;
            else
                return m;
        }
        return l;
    }
    // Increase reference count.
    void Ref() { refs_.fetch_add(1); }

    // Drop reference count.  Delete if no more references exist.
    void Unref() {
        refs_.fetch_add(-1);
        assert(refs_.load() >= 0);
        if (refs_.load() <= 0) {
            delete this;
        }
    }

    ~Bucket() {
        assert(refs_.load() == 0);
        if (db) delete db;
        if (hugepage) delete hugepage;
    }
private:
    
};


// 按照 largest key 从小到大的顺序
struct BucketCmp {
    bool operator() (const Bucket* b1, const Bucket* b2) {
        int r = b1->largest.compare(b2->largest);
        return r < 0;
    }
};

}


#endif
