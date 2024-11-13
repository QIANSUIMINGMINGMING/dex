#pragma once

#include <bitset>
#include <memory>
#include <shared_mutex>

// #include "BufferRing.h"
#include "Common.h"
#include "./bloomfilter/pbf.h"

namespace XMD {

#ifndef FIT_WAY
#define FIT_WAY 7
#endif

constexpr int effective_items = 65536;
constexpr double effective_fpp = 0.01;
constexpr uint64_t lock_table_num = 16;

constexpr int kMaxFilters = 8192;
// constexpr int kMaxSkipListData = 65536;
// constexpr int extra_data_reserved = 100;

// struct SkipListNode {
//   KVTS kvts;
//   uint16_t level;
//   uint16_t next_ptrs[kMaxLevel];
// } __attribute__((packed));

// std::atomic<cutil::ull_t> lock_table[lock_table_num][kMaxSkipListData]
//                                     [kMaxLevel];

struct FilterPoolNode {
  pbf::PageBloomFilter<FIT_WAY> filter{
      pbf::Create<pbf::BestWay(effective_fpp)>(effective_items, effective_fpp)};
  std::atomic<TS> startTS{0};
  std::atomic<TS> endTS{NULL_TS};
  std::vector<u64> remains_offsets;
  std::mutex remains_mutex;

  FilterPoolNode() {
    remains_offsets.reserve(100);
  }

  void insert_remains_offset(u64 offset) {
    std::lock_guard<std::mutex> lock(remains_mutex);
    remains_offsets.push_back(offset);
  }
};

struct FilterNodeBuffer {
  FilterPoolNode filters[kMaxFilters];
  volatile size_t oldest{0};
  std::atomic<size_t> latest{0};
  std::atomic<TS> oldest_TS_;

  FilterNodeBuffer() {
    for (size_t i = 0; i < kMaxFilters; i++) {
      filters[i].endTS = NULL_TS;
    }
    oldest_TS_ = myClock::get_ts();
  }

  inline FilterPoolNode &operator[](int i) { return filters[i]; }

  inline bool get_filter_by_TS(const TS &ts, FilterPoolNode *&filter) {
    if (ts <= oldest_TS_.load()) return false;
    size_t cur = latest;
    while (cur != oldest) {
      if (filters[cur].endTS <= ts) {
        cur = RING_ADD(cur, 1, kMaxFilters);
        break;
      } else {
        cur = RING_SUB(cur, 1, kMaxFilters);
      }
    }
    filter = &filters[cur];
    return true;
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
    size_t cur = latest;
    TS snapshot_ts = oldest_TS_.load();
    FilterPoolNode *filter = &filters[cur];
    size_t hash_kit_1;
    pbf::V128X has_kit_2 = pbf::PageBloomFilter<FIT_WAY>::partial_test1(
        (const uint8_t *)&key, sizeof(Key), &(filter->filter), hash_kit_1);
    //leave false positive to future work
    if (filter->filter.partial_test2(hash_kit_1, has_kit_2)) {
      // maxTS = XMD::myClock::get_ts();
      // cur == oldest ? minTS = snapshot_ts : minTS = filters[RING_SUB(cur, 1, kMaxFilters)].endTS;
      return true;
    }
    while (cur != oldest && snapshot_ts <= filter->endTS) {
      cur = RING_SUB(cur, 1, kMaxFilters);
      filter = &filters[cur];
      if (filter->filter.partial_test2(hash_kit_1, has_kit_2)) {
        // maxTS = filter->endTS;
        // minTS = filter
        return true;
      }
    }
    return false;
  }
};

} // namespace XMD
