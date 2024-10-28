#pragma once

#include <atomic>
#include <mutex>
#include <queue>
#include <random>
#include <shared_mutex>
#include <thread>

#include "ChronoSkipList.h"

namespace XMD {

constexpr uint64_t kIntervalNumber = 1024;
constexpr uint64_t kIntervalSize = 1 << 16 - 1;

class KVCache {
 public:
  MonotonicBufferRing<SkipListNode> chronoBuffer_;
  SkipList *skiplists[kIntervalNumber];
  // std::shared_mutex skiplists_mutex_[kIntervalNumber];
  uint64_t cur_latest = 0;
  uint64_t cur_oldest = 0;

  KVCache(uint64_t size) : chronoBuffer_(size) {
    for (uint64_t i =0; i < kIntervalNumber; i++) {
        skiplists[i] = new SkipList(&chronoBuffer_);
    }
    skiplists[cur_latest]->init_skiplist();
  }

  void insert(const KVTS &kvts) {
    SkipList *insert_skiplist = skiplists[cur_latest];
    if (!insert_skiplist->not_full()) {
      cur_latest = RING_ADD(cur_latest, 1, kIntervalNumber);
      insert_skiplist = skiplists[cur_latest];
      insert_skiplist->init_skiplist();
    }
    insert_skiplist->insert(kvts);
  }

  void search() {
    // TODO
  }

  void update_oldest() {
    skiplists[cur_oldest]->reset_skiplist();
    cur_oldest = RING_ADD(cur_oldest, 1, kIntervalNumber);
  }

  SkipList *get_oldest() {
    SkipList *oldest_skiplist = skiplists[cur_oldest];
    while (RING_SUB(cur_latest, cur_oldest, kIntervalNumber) < 10) {
      sched_yield();
    }
    return oldest_skiplist;
  }
};

}  // namespace XMD

// #include "Common.h"
// #include "FilterPool.h"
// #include "bloomfilter/pbf.h"
// #include "third_party/random.h"

// constexpr int kMaxFilters = 1024 * 1024;

// constexpr int kMaxSkipListData = 2 * 65536;
// constexpr float kCreateDataRatio = 0.9;

// constexpr int directFindBar = 100;

// class KVCache {
//  public:
//   KVCache(size_t cache_size)
//       : cache_size(cache_size), buffer(cache_size), filter_pool(&buffer) {}

//   KVCache(size_t cache_size, bool no_fp)
//       : cache_size(cache_size), buffer(cache_size), filter_pool() {
//     size_t start_offset;
//     auto skp = buffer.alloc(start_offset);
//     std::fill_n(skp->next_ptrs, kMaxLevel, kKeyMax);
//     filter_pool[0].start_offset = start_offset;
//   }

//   void insert(KVTS *kvts) { insert(kvts->k, kvts->ts, kvts->v); }

//   void insert(Key k, TS ts, Value v) {
//     filter_pool.RlockGlobalTS();
//     assert(ts > filter_pool.getGlobalTS());
//     size_t node_offset;
//     int sln_level = randomHeight();
//     auto sln = buffer.alloc(1, node_offset);
//     sln->kvts.k = k;
//     sln->kvts.ts = ts;
//     sln->kvts.v = v;
//     sln->level = sln_level;
//     std::fill_n(sln->next_ptrs, kMaxLevel, kKeyMax);
//     bool filter_is_latest;
//     auto filter = filter_pool.get_filter(ts, filter_is_latest);
//     if (filter_is_latest) {
//       filter->olc_thread_count.fetch_add(1);
//       skiplist_insert_latest(sln, node_offset, filter);
//       filter->olc_thread_count.fetch_sub(1);
//     } else {
//       while (filter->olc_thread_count.load() != 0) {
//       }
//       skiplist_insert(sln, node_offset, filter);
//     }
//     filter->filter->insert(k);
//     filter_pool.RunlockGlobalTS();
//   }

//   void test_buffer_insert(Key k, TS ts, Value v, bool is_latest) {
//     size_t node_offset = 0;
//     int sln_level = randomHeight();
//     // printf("random height:%d\n", sln_level);
//     auto sln = buffer.alloc(1, node_offset);
//     sln->kvts.k = k;
//     sln->kvts.ts = ts;
//     sln->kvts.v = v;
//     sln->level = sln_level;
//     std::fill_n(sln->next_ptrs, kMaxLevel, kKeyMax);
//     FilterPoolNode *filter = &filter_pool[0];
//     // printf("insert %lu", k);
//     if (is_latest) {
//       skiplist_insert_latest(sln, node_offset, filter);
//     } else {
//       skiplist_insert(sln, node_offset, filter);
//     }
//   }

//   bool search(Key k, Value &v) {
//     filter_pool.RlockGlobalTS();
//     bool filter_is_latest;
//     int pre_oldest;
//     int filter_idx = filter_pool.contain(k, filter_is_latest, pre_oldest);
//     if (filter_idx == -1) {
//       filter_pool.RunlockGlobalTS();
//       return false;
//     }
//     assert(filter_pool[filter_idx].endTS > filter_pool.getGlobalTS());
//     auto filter = &filter_pool[filter_idx];
//     if (filter_is_latest) {
//       filter->olc_thread_count.fetch_add(1);
//       if (skiplist_search_latest(k, v, filter)) {
//         filter->olc_thread_count.fetch_sub(1);
//         filter_pool.RunlockGlobalTS();
//         return true;
//       }
//     }
//     while (filter->olc_thread_count.load() != 0) {
//     }
//     while (!skiplist_search(k, v, filter)) {
//       filter_idx = filter_pool.contain(k, filter_idx, pre_oldest);
//       if (filter_idx == -1) {
//         filter_pool.RunlockGlobalTS();
//         return false;
//       }
//       assert(filter_pool[filter_idx].endTS > filter_pool.getGlobalTS());
//       filter = &filter_pool[filter_idx];
//     }
//     filter_pool.RunlockGlobalTS();
//     return true;
//   }

//   void test_buffer_search(Key k, Value &v, bool is_latest) {
//     FilterPoolNode *filter = &filter_pool[0];
//     if (is_latest) {
//       skiplist_search_latest(k, v, filter);
//     } else {
//       skiplist_search(k, v, filter);
//     }
//   }

//   void clear_TS(TS oldest_TS) { filter_pool.updateGlobalTS(oldest_TS); }

//  private:
//   void skiplist_insert(SkipListNode *sln, size_t node_offset,
//                        FilterPoolNode *filter) {
//     // random int level
//     size_t cur_pos = filter->start_offset;
//   restart:
//     bool need_restart = false;
//     auto cur_v =
//         cutil::read_lock_or_restart(filter->skiplist_lock_, need_restart);
//     if (need_restart) {
//       goto restart;
//     }
//     cutil::upgrade_to_write_lock_or_restart(filter->skiplist_lock_, cur_v,
//                                             need_restart);
//     if (need_restart) {
//       goto restart;
//     }
//     auto skiplist_head = &buffer[filter->start_offset];
//     auto p = skiplist_head;
//     auto next_pos = p->next_ptrs[kMaxLevel - 1];
//     // search and insert;
//     for (int i = kMaxLevel - 1; i >= 0; i--) {
//       while (next_pos != kKeyMax && buffer[next_pos].kvts.k < sln->kvts.k) {
//         p = &buffer[next_pos];
//         cur_pos = next_pos;
//         next_pos = p->next_ptrs[i];
//       }
//       if (i < sln->level) {
//         insert_buffer[i] = cur_pos;
//       }
//       if (i > 0) {
//         next_pos = p->next_ptrs[i - 1];
//       }
//     }
//     for (int i = 0; i < sln->level; i++) {
//       auto pre_node = &buffer[insert_buffer[i]];
//       sln->next_ptrs[i] = pre_node->next_ptrs[i];
//       pre_node->next_ptrs[i] = node_offset;
//     }
//     cutil::write_unlock(filter->skiplist_lock_);
//   }

//   inline void unlock_previous(size_t start_pos, int start_level, int level,
//                               uint8_t fliper) {
//     for (int i = start_level; i < level; i++) {
//       size_t pos = insert_buffer[i] - start_pos;
//       cutil::write_unlock(Latest_lock_buffer[fliper][pos][i]);
//     }
//   }

//   void skiplist_insert_latest(SkipListNode *sln, size_t node_offset,
//                               FilterPoolNode *filter) {
//     size_t start_pos = filter->start_offset;
//     size_t cur_pos = start_pos;
//   restart:
//     bool need_restart = false;
//     auto skiplist_head = &buffer[filter->start_offset];
//     auto cur_v = cutil::read_lock_or_restart(
//         Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos]
//                           [kMaxLevel - 1],
//         need_restart);
//     if (need_restart) {
//       goto restart;
//     }
//     auto p = skiplist_head;
//     auto next_pos = p->next_ptrs[kMaxLevel - 1];
//     for (int i = kMaxLevel - 1; i >= 0; i--) {
//       while (next_pos != kKeyMax && buffer[next_pos].kvts.k < sln->kvts.k) {
//         assert(next_pos - start_pos < kMaxSkipListData);
//         p = &buffer[next_pos];
//         auto f_v = cutil::read_lock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][next_pos - start_pos][i],
//             need_restart);
//         if (need_restart) {
//           unlock_previous(start_pos, i + 1, sln->level, filter->my_fliper);
//           goto restart;
//         }
//         cutil::read_unlock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i],
//             cur_v, need_restart);
//         if (need_restart) {
//           unlock_previous(start_pos, i + 1, sln->level, filter->my_fliper);
//           goto restart;
//         }
//         cur_pos = next_pos;
//         next_pos = p->next_ptrs[i];
//         cur_v = f_v;
//       }
//       if (i < sln->level) {
//         cutil::upgrade_to_write_lock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i],
//             cur_v, need_restart);
//         if (need_restart) {
//           unlock_previous(start_pos, i + 1, sln->level, filter->my_fliper);
//           goto restart;
//         }
//         insert_buffer[i] = cur_pos;
//       }
//       if (i > 0) {
//         // lock next level
//         int sig = int(i >= sln->level);
//         auto f_v = cutil::read_lock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i -
//             1], need_restart);
//         if (need_restart) {
//           unlock_previous(start_pos, i + sig, sln->level, filter->my_fliper);
//           goto restart;
//         }
//         cutil::read_unlock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i],
//             cur_v, need_restart);
//         if (need_restart) {
//           unlock_previous(start_pos, i + sig, sln->level, filter->my_fliper);
//           goto restart;
//         }
//         cur_v = f_v;
//         next_pos = p->next_ptrs[i - 1];
//       }
//     }
//     for (int i = 0; i < sln->level; i++) {
//       auto pre_node = &buffer[insert_buffer[i]];
//       sln->next_ptrs[i] = pre_node->next_ptrs[i];
//       pre_node->next_ptrs[i] = node_offset;
//       cutil::write_unlock(
//           Latest_lock_buffer[filter->my_fliper][insert_buffer[i]][i]);
//     }
//   }

//   bool skiplist_search(Key k, Value &v, FilterPoolNode *filter) {
//   restart:
//     bool need_restart = false;
//     auto cur_v =
//         cutil::read_lock_or_restart(filter->skiplist_lock_, need_restart);
//     if (need_restart) {
//       goto restart;
//     }
//     auto skiplist_head = &buffer[filter->start_offset];
//     auto p = skiplist_head;
//     auto next_pos = p->next_ptrs[kMaxLevel - 1];
//     for (int i = kMaxLevel - 1; i >= 0; i--) {
//       while (next_pos != kKeyMax && buffer[next_pos].kvts.k < k) {
//         p = &buffer[next_pos];
//         next_pos = p->next_ptrs[i];
//       }
//       if (next_pos != kKeyMax && buffer[next_pos].kvts.k == k) {
//         v = buffer[next_pos].kvts.v;
//         cutil::read_unlock_or_restart(filter->skiplist_lock_, cur_v,
//                                       need_restart);
//         if (need_restart) {
//           goto restart;
//         }
//         return true;
//       }
//       if (i > 0) {
//         next_pos = p->next_ptrs[i - 1];
//       }
//     }
//     cutil::read_unlock_or_restart(filter->skiplist_lock_, cur_v,
//     need_restart); if (need_restart) {
//       goto restart;
//     }
//     return false;
//   }

//   bool skiplist_search_latest(Key k, Value &v, FilterPoolNode *filter) {
//     size_t start_pos = filter->start_offset;
//     size_t cur_pos = start_pos;
//     size_t cur_offset;
//     if (buffer.check_distance(start_pos, cur_offset) < directFindBar) {
//       for (size_t i = start_pos; i < cur_offset; i++) {
//         if (buffer[i].kvts.k == k) {
//           v = buffer[i].kvts.v;
//           return true;
//         }
//       }
//       return false;
//     }
//   restart:
//     bool need_restart = false;
//     auto skiplist_head = &buffer[filter->start_offset];
//     auto cur_v = cutil::read_lock_or_restart(
//         Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos]
//                           [kMaxLevel - 1],
//         need_restart);
//     if (need_restart) {
//       goto restart;
//     }
//     auto p = skiplist_head;
//     auto next_pos = p->next_ptrs[kMaxLevel - 1];
//     for (int i = kMaxLevel - 1; i >= 0; i--) {
//       while (next_pos != kKeyMax && buffer[next_pos].kvts.k < k) {
//         p = &buffer[next_pos];
//         auto f_v = cutil::read_lock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][next_pos - start_pos][i],
//             need_restart);
//         if (need_restart) {
//           goto restart;
//         }
//         cutil::read_unlock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i],
//             cur_v, need_restart);
//         if (need_restart) {
//           goto restart;
//         }
//         cur_pos = next_pos;
//         next_pos = p->next_ptrs[i];
//         cur_v = f_v;
//       }
//       if (next_pos != kKeyMax && buffer[next_pos].kvts.k == k) {
//         v = buffer[next_pos].kvts.v;
//         cutil::read_unlock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i],
//             cur_v, need_restart);
//         if (need_restart) {
//           goto restart;
//         }
//         return true;
//       }
//       if (i > 0) {
//         auto f_v = cutil::read_lock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i -
//             1], need_restart);
//         if (need_restart) {
//           goto restart;
//         }
//         cutil::read_unlock_or_restart(
//             Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i],
//             cur_v, need_restart);
//         if (need_restart) {
//           goto restart;
//         }
//         cur_v = f_v;
//         next_pos = p->next_ptrs[i - 1];
//       }
//     }
//     return false;
//   }

//   int randomHeight() {
//     auto rnd = Random::GetTLSInstance();

//     // Increase height with probability 1 in kBranching
//     int height = 1;
//     while (height < kMaxLevel && rnd->Next() < (Random::kMaxNext + 1) / 4) {
//       height++;
//     }
//     assert(height > 0);
//     assert(height <= kMaxLevel);
//     return height;
//   }

//   size_t cache_size;
//   MonotonicBufferRing<SkipListNode> buffer;
//   std::atomic<cutil::ull_t>
//   Latest_lock_buffer[2][kMaxSkipListData][kMaxLevel]; FilterPool filter_pool;
//   static thread_local size_t insert_buffer[kMaxLevel];
// };