#pragma once

#include <boost/unordered/concurrent_flat_map.hpp>
#include <boost/unordered/unordered_map.hpp>
#include <iostream>


// #include "Common.h"
// #include "Directory.h"
// #include "GlobalAddress.h"
// #include "KVCache.h"

namespace forest {

// struct BSearchResult {
//   bool is_leaf;
//   uint8_t level;
//   GlobalAddress next_level;
//   Value val;
// };

// class BHeader {
//  private:
//   GlobalAddress leftmost_ptr;
//   GlobalAddress parent_ptr;
//   uint8_t level;
//   int16_t last_index;
//   Key lowest;
//   Key highest;
//   uint8_t invalidate;

//   friend class BInternalPage;
//   friend class BLeafPage;
//   friend class BForest;
//   friend class IndexCache;

//  public:
//   BHeader() {
//     leftmost_ptr = GlobalAddress::Null();
//     parent_ptr = GlobalAddress::Null();
//     last_index = -1;
//     lowest = kKeyMin;
//     highest = kKeyMax;
//   }

//   BHeader(BHeader &hdr) {
//     leftmost_ptr = hdr.leftmost_ptr;
//     parent_ptr = hdr.parent_ptr;
//     lowest = hdr.lowest;
//     highest = hdr.highest;
//   }

//   void debug() const {
//     std::cout << "leftmost=" << leftmost_ptr
//               << ", "
//               // << "sibling=" << sibling_ptr << ", "
//               << "level=" << (int)level << ","
//               << "cnt=" << last_index + 1 << ","
//               << "range=[" << lowest << " - " << highest << "]";
//   }
// } __attribute__((packed));
// ;

// class BInternalEntry {
//  public:
//   Key key;
//   GlobalAddress ptr;

//   BInternalEntry() {
//     ptr = GlobalAddress::Null();
//     key = 0;
//   }
// } __attribute__((packed));

// class BLeafEntry {
//  public:
//   Key key;
//   Value value;

//   BLeafEntry() {
//     value = kValueNull;
//     key = 0;
//   }
// } __attribute__((packed));

// constexpr int kBInternalCardinality =
//     (kInternalPageSize - sizeof(BHeader)) / sizeof(BInternalEntry);

// constexpr int kBLeafCardinality =
//     (kLeafPageSize - sizeof(BHeader)) / sizeof(BLeafEntry);

// class BInternalPage {
//  private:
//   uint32_t crc;
//   uint8_t front_version;
//   BHeader hdr;
//   BInternalEntry records[kBInternalCardinality];
//   uint8_t rear_version;

//   friend class BForest;
//   friend class IndexCache;

//  public:
//   // this is called when tree grows
//   BInternalPage(GlobalAddress left, const Key &key, GlobalAddress right,
//                 uint32_t level = 0) {
//     hdr.leftmost_ptr = left;
//     hdr.level = level;
//     records[0].key = key;
//     records[0].ptr = right;
//     records[1].ptr = GlobalAddress::Null();

//     hdr.last_index = 0;
//   }

//   BInternalPage(uint32_t level = 0) {
//     hdr.level = level;
//     records[0].ptr = GlobalAddress::Null();
//   }

//   void set_consistent() {
//     this->crc =
//         CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//   }

//   bool check_consistent() const {
//     bool succ = true;
//     auto cal_crc =
//         CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//     succ = cal_crc == this->crc;
//     return succ;
//   }

//   void debug() const {
//     std::cout << "InternalPage@ ";
//     hdr.debug();
//   }

//   void verbose_debug() const {
//     this->debug();
//     for (int i = 0; i < this->hdr.last_index + 1; ++i) {
//       printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
//     }
//     printf("\n");
//   }

// } __attribute__((packed));

// class BLeafPage {
//  private:
//   uint32_t crc;
//   uint8_t front_version;
//   BHeader hdr;
//   BLeafEntry records[kBLeafCardinality];
//   uint8_t rear_version;

//   friend class BForest;

//  public:
//   BLeafPage(uint32_t level = 0) {
//     hdr.level = level;
//     records[0].value = kValueNull;
//   }

//   void set_consistent() {
//     this->crc =
//         CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//   }

//   bool check_consistent() const {
//     bool succ = true;
//     auto cal_crc =
//         CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//     succ = cal_crc == this->crc;
//     return succ;
//   }

//   void debug() const {
//     std::cout << "LeafPage@ ";
//     hdr.debug();
//   }

// } __attribute__((packed));

// enum class BatchInsertFlag {
//   LEFT_SAME_PAGE,
//   LEFT_DIFF_PAGE,
//   RIGHT_SAME_PAGE,
//   RIGHT_DIFF_PAGE
// };

// struct BInsertedEntry {
//   BInternalEntry entry;
//   GlobalAddress page_addr;
// };

// class BForest {
//  public:
//   BForest(DSM *dsm, int CNs, uint16_t tree_id);

//   bool search(const Key &k, Value &v, CoroContext *cxt = nullptr,
//               int coro_id = 0);

//   void batch_insert(KVTS *kvs, int full_number, int thread_num);

//  private:
//   DSM *dsm;
//   int tree_num;
//   uint16_t tree_id;
//   GlobalAddress root_ptr_ptr[MAX_COMP];
//   IndexCache *indexCaches[MAX_COMP];
//   int cache_sizes[MAX_COMP];
//   std::atomic<int> cur_cache_sizes[MAX_COMP];
//   GlobalAddress allocator_starts[MAX_MEMORY];

//   thread_local static uintptr_t stack_page_buffer[define::kMaxLevelOfTree];
//   thread_local static GlobalAddress
//       stack_page_addr_buffer[define::kMaxLevelOfTree];

//   // boost::unordered_map<uint64_t, std::atomic<int>> tree_meta;
//   boost::concurrent_flat_map<uint64_t, int> tree_meta;
//   // new level
//   GlobalAddress next_level_page_addr;
//   BInternalPage *next_level_page;

//   boost::concurrent_flat_map<uint64_t, std::vector<BInternalEntry>>
//       new_inserted_entries[define::kMaxLevelOfTree];
//   std::unordered_map<uint64_t, BHeader> new_inserted_headers;
//   std::thread batch_insert_threads[kMaxBatchInsertCoreNum];

//  private:
//   static void batch_insert_leaf(BForest *forest, KVTS *kvs, int start, int cnt,
//                                 int full_number, int thread_id,
//                                 CoroContext *cxt = nullptr, int coro_id = 0);
//   void batch_insert_internal(int thread_num);

//   GlobalAddress get_root_ptr_ptr(uint16_t id);
//   GlobalAddress get_root_ptr(CoroContext *cxt, int coro_id, uint16_t id);

//   void set_stack_buffer(int level, const Key &k, CoroContext *cxt, int coro_id);
//   void search_stack_buffer(int level, const Key &k, GlobalAddress &result);

//   void split_leaf(KVTS *kvs, int start, int split_num, int range,
//                   BatchInsertFlag l_flag, BatchInsertFlag r_flag, int thread_id,
//                   CoroContext *cxt = nullptr, int coro_id = 0);
//   inline void go_in_leaf(BLeafPage *lp, int start, Key lowest, Key highest,
//                          int &next);
//   inline void go_in_kvts(KVTS *kvs, int start, int range, int &next);
//   void calculate_meta(int split_num, BatchInsertFlag l_flag);

//   inline void set_leaf(KVTS *kvs, const Key &k, const Value &v,
//                        bool &need_split, BatchInsertFlag l_flag,
//                        BatchInsertFlag r_flag, int pre_kv, int pro_kv,
//                        CoroContext *cxt = nullptr, int coro_id = 0);

//   void update_root(int level);

//   bool page_search(GlobalAddress page_addr, const Key &k, BSearchResult &result,
//                    CoroContext *cxt, int coro_id);

//   void internal_page_search(BInternalPage *page, const Key &k,
//                             BSearchResult &result);

//   void leaf_page_search(BLeafPage *page, const Key &k, BSearchResult &result);

//   bool check_ga(GlobalAddress ga);
// };
};  // namespace forest
// namespace forest
