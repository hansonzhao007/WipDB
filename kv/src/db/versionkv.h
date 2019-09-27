#pragma once


#include <map>
#include <set>
#include <vector>
#include <unordered_set>
#include <memory>

#include "db/dbformat.h"
#include "db/bucket.h"
#include "port/port.h"
#include "kv/options.h"


namespace kv {

class VersionKVEdit;
class VersionKV;
class VersionSetKV;

class VersionKVEdit {
public:
    void AddBucket(Bucket* bucket) {
        buckets_add_.push_back(bucket);
    }

    void DelBucket(Bucket* bucket) {
        bucket_del_ = bucket;
    }

private:
    friend class VersionSetKV;

    Bucket* bucket_del_;
    std::vector<Bucket*> buckets_add_;
    
};

class VersionKV {
public:
    void Ref();
    void Unref();

    std::string BucketsInfo();
    std::vector<Bucket*> buckets_;

    Bucket* FindOldBucket(Bucket* bucket) {
        auto B = *std::lower_bound(buckets_.begin(), buckets_.end(), bucket, BucketCmp());
        return B->bucket_old;
    }

private:
    friend class VersionSetKV;

    VersionSetKV* vset_;            // VersionSet to which this Version belongs
    VersionKV* next_;               // Next version in linked list
    VersionKV* prev_;               // Previous version in linked list
    int refs_;                    // Number of live refs to this version

    
    explicit VersionKV(VersionSetKV* vset);
    ~VersionKV();

    // No copying allowed
    VersionKV(const VersionKV&);
    void operator=(const VersionKV&);

};

class VersionSetKV {
public:
    VersionSetKV(const std::string& dbname,
             const Options* options);
    ~VersionSetKV();

    VersionKV* current() const { return current_; }
    void Apply(VersionKVEdit* edit);
    int VersionCount();
    
private:
    friend class VersionKV;
    void AppendVersion(VersionKV* v);

    Env* const env_;
    const std::string dbname_;
    const Options* const options_;
    VersionKV dummy_versions_;  // Head of circular doubly-linked list of versions.
    VersionKV* current_;        // == dummy_versions_.prev_
};


}
