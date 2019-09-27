#include "db/versionkv.h"
#include "kv/env.h"
#include <cassert>

namespace kv {

VersionKV::VersionKV(VersionSetKV* vset)
    : vset_(vset), next_(this), prev_(this), refs_(0)
    {

    }


VersionKV::~VersionKV() {
  assert(refs_ == 0);
  prev_->next_ = next_;
  next_->prev_ = prev_;

}


void VersionKV::Ref() {
  ++refs_;
}

void VersionKV::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

std::string VersionKV::BucketsInfo() {
  std::string res("=== Largest Key Order:\n");
    for(auto& b: buckets_) {
        res += b->largest + " (" + std::to_string(b->refs_.load())+"), " ;
    }
    res += "\n";
    return res;
}


VersionSetKV::VersionSetKV(const std::string& dbname,
                       const Options* options)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      dummy_versions_(this),
      current_(nullptr) {
  AppendVersion(new VersionKV(this));
}
VersionSetKV::~VersionSetKV() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
}

void VersionSetKV::Apply(VersionKVEdit* edit) {
  VersionKV* v = new VersionKV(this);
  v->buckets_ = current_->buckets_; // copy
  Log(Env::Default()->info_log_, "=== KV old pivot. === %s", v->BucketsInfo().c_str());

  auto iter = v->buckets_.begin();

  // remove old bucket from version
  while (iter != v->buckets_.end()) {
    if (*iter == edit->bucket_del_) {
      v->buckets_.erase(iter);
      break;
    }
    iter++;
  }

  for (auto& b : edit->buckets_add_) {
    v->buckets_.push_back(b);
  }

  std::sort(v->buckets_.begin(), v->buckets_.end(), BucketCmp());

  AppendVersion(v);
  Log(Env::Default()->info_log_, "=== KV updated pivot. === %s", current_->BucketsInfo().c_str());
}

int VersionSetKV::VersionCount() {
  VersionKV* cur = &dummy_versions_;
  int count = 0;
  while (cur->next_ && cur->next_ != &dummy_versions_) {
    count++;
    cur = cur->next_;
  }
  return count;
}

void VersionSetKV::AppendVersion(VersionKV* v) {
  // 将当前 version 插入到 version list里面，并将 v 设置为 current
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) { // 这里用来删除旧 version
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

}