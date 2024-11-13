#include "../third_party/libcuckoo/cuckoohash_map.hh"
#include "libfadecuckoo/faded_cuckoohash_map.hh"

#include "../Common.h"
#include "../DSM.h"
#include "ChronoBuffer.h"
#include "Filters.h"



namespace XMD {

constexpr int kMaxRequestThreads = 256;
constexpr int defaultCacheSize = (128 * define::MB) / sizeof(KVTS);
constexpr int kAllocateChunk = 32;
constexpr int remote_rpc_batch = 16;

namespace RequestCache_v3 {
using RequestHashTable = libfadecuckoo::cuckoohash_map<Key, Value>;

class RequestCache {
public:
  RequestHashTable rht_;
  u64 chrono_size_;
  u64 ht_size_;
  std::atomic<u64> cur_ht_size_{0};
  std::thread software_threads[kMaxRequestThreads];
  u64 bucket_num_;
  uint16_t slot_per_bucket_;
  uint64_t node_id_;
  u64 node_num_;
  MonotonicBufferRing<KVTS> chrono_buffer_;
  FilterNodeBuffer filter_buffer_;
  DSM *dsm_;
  TS oldest_ts_;

  static thread_local u64 cur_buffer_start;
  static thread_local u64 cur_buffer_remain;
  static thread_local std::vector<KVTS> remote_list;

  RequestCache(DSM *dsm, u64 chrono_size, int thread_num, u64 node_id,
               u64 node_num)
      : rht_(defaultCacheSize), chrono_buffer_(chrono_size), filter_buffer_(),
        dsm_(dsm) {
    chrono_size_ = chrono_size;
    node_id_ = node_id;
    node_num_ = node_num;
    bucket_num_ = rht_.bucket_count();
    slot_per_bucket_ = rht_.slot_per_bucket();
    ht_size_ = bucket_num_ * slot_per_bucket_;
    oldest_ts_ = myClock::get_ts();
  };

  bool lookup(const Key &k, Value &v) {
    bool found;
    TS ts_in_local;
    found = rht_.find(k, ts_in_local, v);
    if (found) {
      return true;
    } else {
      // path 1
      TS min;
      TS max;
      if (filter_buffer_.contain(k,max, min)) {
        KVTS fetched_kvts;
        int ret = dsm_->rpc_xmd_lookup(k, v, fetched_kvts);
        return ret >= 1 ? true : false;
      }
      return false;
    }
  }

  u64 push_to_chrono_buffer(const KVTS &kvts) {
    if (cur_buffer_remain == 0) {
      chrono_buffer_.alloc(kAllocateChunk, cur_buffer_start);
      cur_buffer_remain = kAllocateChunk;
    }
    u64 cur_push_pos = cur_buffer_start + kAllocateChunk - cur_buffer_remain;
    cur_buffer_remain--;
    auto &cur_kvts = chrono_buffer_[cur_push_pos];
    cur_kvts.k = kvts.k;
    cur_kvts.v = kvts.v;
    cur_kvts.ts = kvts.ts;
    return cur_push_pos;
  }

  void insert(const KVTS &kvts) {
    if (node_id_ == kvts.k % node_num_) {
      if (kvts.ts < oldest_ts_)
        return;
      FilterPoolNode *filter;
      filter_buffer_.get_filter_by_TS(kvts.ts, filter);
      u64 offset = push_to_chrono_buffer(kvts);
      if (filter->endTS.load() != NULL_TS) {
        filter->insert_remains_offset(offset);
      }
    }
    bool found;
    TS got_ts;
    Value got_val;
    found = rht_.find(kvts.k, got_ts, got_val);
    if (found) {
      if (got_ts >= kvts.ts) {
        return;
      } else {
        bool update_success = rht_.update(kvts.k, kvts.ts, kvts.v);
        if (!update_success) {
          bool need_resize = false;
          rht_.insert(kvts.k, kvts.ts, need_resize, kvts.v);
          if (need_resize) {
            // insert to remote
          }
        }
      }
    } else {
      bool need_resize = false;
      rht_.insert(kvts.k, kvts.ts, need_resize, kvts.v);
      if (need_resize) {
        //insert to remote
      }
    }
  }
};
}

struct TSV {
  std::atomic<cutil::ull_t> lock_;
  TS ts;
  Value v;
};

using RequestHashTable = libcuckoo::cuckoohash_map<Key, TSV *>;

class RequestCache {
 public:
  RequestHashTable rht_;
  u64 chrono_size_;
  u64 ht_size_;
  std::atomic<u64> cur_ht_size_{0};
  std::thread software_threads[kMaxRequestThreads];
  TS oldest_ts_;
  u64 bucket_num_;
  uint16_t slot_per_bucket_;
  uint64_t node_id_;
  u64 node_num_;
  MonotonicBufferRing<KVTS> chrono_buffer_;
  FilterNodeBuffer filter_buffer_;
  DSM *dsm_;

  static thread_local u64 cur_buffer_start;
  static thread_local u64 cur_buffer_remain;
  static thread_local std::vector<KVTS> remote_list;

  RequestCache(DSM *dsm, u64 chrono_size, int thread_num, u64 node_id,
               u64 node_num)
      : rht_(defaultCacheSize),
        chrono_buffer_(chrono_size),
        filter_buffer_(),
        dsm_(dsm) {
    chrono_size_ = chrono_size;
    node_id_ = node_id;
    node_num_ = node_num;
    bucket_num_ = rht_.bucket_count();
    slot_per_bucket_ = rht_.slot_per_bucket();
    ht_size_ = bucket_num_ * slot_per_bucket_;
    oldest_ts_ = myClock::get_ts();
  };

  static bool my_fn(TSV *tsv_value, TS ots) {
    assert(tsv_value != nullptr);
    return tsv_value->ts <= ots;
  }

  int sample_erase(int num_erase = 3) {
    int cur_erase = 0;
    int i = 0;
    while (i < num_erase) {
      // 64 bit random key in key space
      Key random_k = rand();
      cur_erase += rht_.erase_fn_custom(random_k, my_fn, oldest_ts_);
      i++;
    }
    return cur_erase;
  }

  int sample_erase_with_lock() {
    // 64 bit random key in key space
    int cur_erase = 0;
    auto lock_table = rht_.lock_table();
    auto it = lock_table.begin();
    while (it != lock_table.end()) {
      if (it->second->ts <= oldest_ts_) {
        rht_.erase(it->first);
        ++cur_erase;
      } else {
        ++it;
      }
    }
    lock_table.unlock();
    return cur_erase;
  }

  bool lookup(const Key &k, Value &v) {
    TSV *tsv = nullptr;
    bool found;
    found = rht_.find(k, tsv);
    if (found) {
      v = tsv->v;
      return true;
    } else {
      // path 1
      TS min;
      TS max;
      if (filter_buffer_.contain(k, max, min)) {
        KVTS fetched_kvts;
        int ret = dsm_->rpc_xmd_lookup(k, v, fetched_kvts);
        return ret >= 1 ? true : false;
      }
      return false;
    }
  }

  u64 push_to_chrono_buffer(const KVTS &kvts) {
    if (cur_buffer_remain == 0) {
      chrono_buffer_.alloc(kAllocateChunk, cur_buffer_start);
      cur_buffer_remain = kAllocateChunk;
    }
    u64 cur_push_pos = cur_buffer_start + kAllocateChunk - cur_buffer_remain;
    cur_buffer_remain--;
    auto &cur_kvts = chrono_buffer_[cur_push_pos];
    cur_kvts.k = kvts.k;
    cur_kvts.v = kvts.v;
    cur_kvts.ts = kvts.ts;
    return cur_push_pos;
  }

  void insert(const KVTS &kvts) {
    if (node_id_ == kvts.k % node_num_) {
      if (kvts.ts < oldest_ts_) return;
      FilterPoolNode *filter;
      filter_buffer_.get_filter_by_TS(kvts.ts, filter);
      u64 offset = push_to_chrono_buffer(kvts);
      if (filter->endTS.load() != NULL_TS) {
        filter->insert_remains_offset(offset);
      }
    }

    TSV *tsv = nullptr;
    bool found;
    found = rht_.find(kvts.k, tsv);
    if (found) {
      bool restart;
    restart:
      cutil::ull_t version = cutil::read_lock_or_restart(tsv->lock_, restart);
      if (restart) {
        goto restart;
      }
      if (tsv->ts >= kvts.ts) {
        return;
      } else {
        cutil::upgrade_to_write_lock_or_restart(tsv->lock_, version, restart);
        if (restart) {
          goto restart;
        }
        tsv->ts = kvts.ts;
        tsv->v = kvts.v;
        cutil::write_unlock(tsv->lock_);
      }
    } else {
      u64 old = cur_ht_size_.load();
      if (old >= ht_size_ / 2 && old < ht_size_) {
        rht_.insert(kvts.k, new TSV{0, kvts.ts, kvts.v});
        cur_ht_size_.fetch_add(1);
        if (rand() % 10 == 0) {
          int erase_num = sample_erase();
          if (erase_num > 0) {
            cur_ht_size_.fetch_sub(erase_num);
          }
        }
      } else if (old >= ht_size_) {
        filter_buffer_.insert_filter(kvts);
        // dsm_->rpc_xmd_update(kvts);
        // int erase_num = sample_erase(100);
        // cur_ht_size_.fetch_sub(erase_num);
        if (remote_list.size() == remote_rpc_batch) {
          dsm_->rpc_xmd_update(kvts);
          int erase_num = sample_erase(100);
          cur_ht_size_.fetch_sub(erase_num);
          remote_list.clear();
        } else {
          remote_list.push_back(kvts);
        }
      } else {
        rht_.insert(kvts.k, new TSV{0, kvts.ts, kvts.v});
        cur_ht_size_.fetch_add(1);
      }
    }
  }
};

}  // namespace XMD
