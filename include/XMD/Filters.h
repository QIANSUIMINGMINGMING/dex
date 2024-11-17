#pragma once

#include <atomic>
#include <bitset>
#include <cstddef>
#include <memory>
#include <shared_mutex>

// #include "BufferRing.h"
#include "../Common.h"
#include "./bloomfilter/pbf.h"

namespace XMD {

#ifndef FIT_WAY
#define FIT_WAY 7
#endif

constexpr int effective_items = 4096;
constexpr double effective_fpp = 0.01;

//leave memory control and handle of false positive of bf to future work
constexpr int kMaxFilters = 8192;

#define INVALID_FILTER 0
#define IMMUTABLE_FILTER 1
#define MUTABLE_FILTER 2

struct FilterPoolNode {
  u64 start_offset{0};
  u64 end_offset{0};
  pbf::PageBloomFilter<FIT_WAY> filter{
      pbf::Create<pbf::BestWay(effective_fpp)>(effective_items, effective_fpp)};
  std::atomic<TS> startTS{0};
  std::atomic<TS> endTS{NULL_TS};
  std::atomic<int> state{0};//0: invalid, 1: immutable, 2: mutable
  std::vector<u64> remains_offsets;
  std::mutex remains_mutex;

  FilterPoolNode() {
    remains_offsets.reserve(100);
  }

  void valid(TS start_ts, u64 cur_offset) {
    start_offset = cur_offset;
    state.store(MUTABLE_FILTER);
    startTS.store(start_ts);
  }

  void insert_remains_offset(u64 offset) {
    std::lock_guard<std::mutex> lock(remains_mutex);
    remains_offsets.push_back(offset);
  }

  void clear_self() {
    start_offset = 0;
    end_offset = 0;
    filter.clear();
    startTS.store(0);
    endTS.store(NULL_TS);
    state.store(INVALID_FILTER);
    remains_offsets.clear();
  }
};

struct FilterNodeBuffer {
  FilterPoolNode filters[kMaxFilters];
  std::atomic<size_t> oldest{0};
  std::atomic<size_t> latest{0};
  std::atomic<TS> oldest_TS_;

  // static thread_local size_t inline local_latest{0};
  // static thread_local size_t inline local_oldest{0};

  FilterNodeBuffer() {
    oldest_TS_ = 0;
    filters[0].valid(0, 0);
  }

  void create_new_filter(u64 cur_offset) {
    size_t cur = latest;
    size_t next = RING_ADD(cur, 1, kMaxFilters);
    filters[cur].endTS.store(XMD::myClock::get_ts());
    filters[cur].end_offset = cur_offset;
    filters[next].valid(filters[cur].endTS, cur_offset);
  }

  FilterPoolNode *get_oldest_filter_and_set_invalid() {
    assert(latest != oldest);
    size_t cur = oldest;
    filters[cur].state.store(IMMUTABLE_FILTER);
    // oldest = next;
    return &filters[cur];
  }

  void delete_old_filter() {
    assert(latest != oldest);
    size_t cur = oldest;
    size_t next = RING_ADD(cur, 1, kMaxFilters);
    //TODO: free
    oldest = next;
    oldest_TS_ = filters[next].startTS.load();
  }

  inline FilterPoolNode &operator[](int i) { return filters[i]; }

  inline bool get_filter_by_TS(const TS &ts, FilterPoolNode *&filter) {
    TS snapshot_ts = oldest_TS_.load();
    size_t snapshot_latest = latest.load();
    size_t snapshot_oldest = oldest.load();

    if (ts <= snapshot_ts) return false;
    size_t cur = snapshot_latest;
    while (cur >= snapshot_oldest) {
      if (filters[cur].endTS > ts && filters[cur].startTS <= ts) {
        filter = &filters[cur];
        return filter->state.load() == MUTABLE_FILTER;
      } else {
        cur = RING_SUB(cur, 1, kMaxFilters);
      }
    }
    assert(false);
    return false;
  }

  bool insert_filter(const KVTS & kvts) {
    FilterPoolNode *filter;
    if (get_filter_by_TS(kvts.ts, filter)) {
      filter->filter.set((const uint8_t *)&kvts.k, sizeof(Key));
      return true;
    }
    return false;
  }

  bool contain(const Key &key, TS & maxTS, TS & minTS) {
    size_t snapshot_latest = latest.load();
    size_t snapshot_oldest = oldest.load();
    TS snapshot_ts = oldest_TS_.load();

    size_t cur = snapshot_latest;
    FilterPoolNode *filter = &filters[cur];
    size_t hash_kit_1;
    pbf::V128X has_kit_2 = pbf::PageBloomFilter<FIT_WAY>::partial_test1(
        (const uint8_t *)&key, sizeof(Key), &(filter->filter), hash_kit_1);
    
    // leave false positive to future work since it's rare
    while (cur >= snapshot_oldest) {
      filter = &filters[cur];
      if (filter->filter.partial_test2(hash_kit_1, has_kit_2)) {
        maxTS = filter->endTS.load();
        minTS = filter->startTS.load();
        return filter->state.load() != INVALID_FILTER;
      }
    }
    return false;
  }
};

} // namespace XMD
