#include <../third_party/libcuckoo/cuckoohash_map.hh>

#include "../Common.h"

namespace XMD {

struct TSV {
  std::atomic<cutil::ull_t> lock_;
  TS ts;
  Value v;
};

using RequestHashTable = libcuckoo::cuckoohash_map<Key, TSV *>;

struct RequestTSTable {
  std::unordered_map<Key, std::atomic<TS>> table_;

  int updateTS(const KVTS &kvts) {
    auto it = table_.find(kvts.k);
    if (it == table_.end()) {
      std::pair<Key, std::atomic<TS>> new_pair;
      new_pair.first = kvts.k;
      new_pair.second.store(kvts.ts);
      table_.emplace(new_pair);
      return 0;
    } else if (it->second.load() >= kvts.ts) {
      return -1;
    } else {
      it->second.store(kvts.ts);
      return 1;
    }
    // if ( < kvts.ts);
  }
};

constexpr int kMaxRequestThreads = 256;
constexpr int defaultCacheSize = (128 * define::MB) / sizeof(KVTS);

class RequestCache {
 public:
  RequestHashTable rht_;
  u64 chrono_size_;
  u64 ht_size_;
  u64 cur_ht_size_{0};
  std::thread software_threads[kMaxRequestThreads];

  RequestCache(u64 chrono_size, u64 ht_size_, int thread_num)
      : rht_(defaultCacheSize) {

        };

  void sample_erase() {}

  void insert(const KVTS &kvts) {
    TSV * tsv = nullptr;
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
      
    }
  }
};

}  // namespace XMD
