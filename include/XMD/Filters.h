#pragma once

#include <bitset>
#include <memory>
#include <shared_mutex>

#include "BufferRing.h"
#include "Common.h"
#include "./bloomfilter/pbf.h"

#ifndef FIT_WAY
#define FIT_WAY 7
#endif

constexpr int effective_items = 65536;
constexpr double effective_fpp = 0.01;
constexpr uint64_t lock_table_num = 16;

constexpr int kMaxFilters = 1024;
// constexpr float kCreateDataRatio = 0.9;

// inline skiplist relevant
constexpr int kMaxLevel = 8;
constexpr int kMaxSkipListData = 65536;
constexpr int extra_data_reserved = 100;

struct SkipListNode {
  KVTS kvts;
  uint16_t level;
  uint16_t next_ptrs[kMaxLevel];
} __attribute__((packed));

std::atomic<cutil::ull_t> lock_table[lock_table_num][kMaxSkipListData]
                                    [kMaxLevel];

struct FilterPoolNode {
  pbf::PageBloomFilter<FIT_WAY> filter{
      pbf::Create<pbf::BestWay(effective_fpp)>(effective_items, effective_fpp)};
  u64 start_offset{NULL_TS};
  volatile TS endTS{NULL_TS};
  volatile bool validate{false};
  std::atomic<cutil::ull_t>
      skiplist_lock_;  // after endTS is set, this lock is used
  int lock_table_id{-1};
  std::vector<u64> extra_data_pos;  // offset
  int extra_data_count{0};
  FilterPoolNode() { extra_data_pos.reserve(extra_data_reserved); }
};

struct FilterNodeBuffer {
  FilterPoolNode filters[kMaxFilters];
  int next_lock_table_id{0};
  volatile size_t oldest{0};
  volatile size_t latest{0};
  inline bool isEmpty();
  inline bool isFull();

  void create(MonotonicBufferRing<SkipListNode> *buffer);
  void release(int n);

  inline FilterPoolNode &operator[](int i) { return filters[i]; }
};

class FilterPool {
 public:
  FilterPool(MonotonicBufferRing<SkipListNode> *buffer);
  FilterPool() = delete;

  int contain(Key key);
  int contain(Key key, bool &is_latest, int &cur_oldest);
  int contain(Key key, int start, int pre_oldest);
  FilterNodeBuffer *get_buffer() { return &fnb; }

  inline void updateGlobalTS(TS ts) {
    TS_mutex_.lock();
    oldest_time = ts;
    TS_mutex_.unlock();
  }

  inline void RlockGlobalTS() { TS_mutex_.lock_shared(); }
  inline void RunlockGlobalTS() { TS_mutex_.unlock_shared(); }
  inline void start_clearer() { clear_blocker.store(1); }
  inline TS getGlobalTS() { return oldest_time; }

 private:
  static constexpr int filter_thread_interval = 1000000;  // ns
  static void clear_thread(FilterPool *fp);
  static void filter_thread(FilterPool *fp,
                            MonotonicBufferRing<SkipListNode> *buffer);
  static void buffer_thread(FilterPool *fp,
                            MonotonicBufferRing<SkipListNode> *buffer);

  FilterNodeBuffer fnb;
  std::thread bufferClearer;
  std::thread filterCreator;
  std::thread releaser;
  std::queue<size_t> delay_free_queue;
  std::shared_mutex TS_mutex_;
  std::atomic<int> clear_blocker{0};
  TS oldest_time{START_TS};
};