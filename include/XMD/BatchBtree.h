#pragma once

#include <city.h>

// #include <boost/unordered/concurrent_flat_map.hpp>
// #include <boost/unordered/unordered_map.hpp>
#include <iostream>

#include "../DSM.h"
#include "../GlobalAddress.h"

namespace XMD {

using Key = uint64_t;
using Value = uint64_t;
constexpr int kPageSize = 1024;

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };
static const uint64_t swizzle_tag = 1ULL << 63;
static const uint64_t swizzle_hide = (1ULL << 63) - 1;
static const uint64_t megaLevel =
    4;  // 4 level as a Bigger Node to do coarse-grained distribution
// Level 0, 1, ..., MegaLevel -1 are grouped as a sub-tree

// crc kv
constexpr int kInternalCardinality =
    (kPageSize - sizeof(uint32_t)) / (sizeof(Key) + sizeof(GlobalAddress));

constexpr int kLeafCardinality =
    (kPageSize - sizeof(uint32_t)) / sizeof(Key) + sizeof(Value);

class NodePage {
 public:
  uint32_t crc = 0;
  uint8_t front_version = 0;
  Value values[kLeafCardinality]{0};
  Key keys[kLeafCardinality]{0};
  uint8_t rear_version = 0;

  void set_consistent() {
    this->crc = CityHash32((char *)values, (&rear_version) - (&front_version));
  }

  bool check_consistent() const {
    bool succ = true;
    auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
    return succ;
  }
};

class RootPtr {
 public:
  uint32_t crc = 0;
  uint8_t front_version = 0;
  GlobalAddress rootptr = 0;
  uint8_t rear_version = 0;
  void set_consistent() {
    this->crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
  }

  bool check_consistent() const {
    bool succ = true;
    auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
    return succ;
  }
};

class BatchBTree {
 public:
  BatchBTree(DSM *dsm, uint64_t tree_id) {
    std::cout << "start generating tree " << tree_id << std::endl;
    dsm_ = dsm;
    tree_id_ = tree_id;
    is_mine_ = dsm_->getMyNodeID() == tree_id ? true : false;
    root_ptr_ptr_ = get_root_ptr_ptr();
  }

  bool search() {}

  void batch_insert() { assert(is_mine_); }

  void bulk_load() { assert(dsm_->getMyNodeID() == 0); }

  DSM *dsm_;
  NodePage *root_;
  GlobalAddress root_ptr_ptr_;
  GlobalAddress root_ptr_;

  uint64_t tree_id_;
  bool is_mine_;

  GlobalAddress get_root_ptr_ptr() {
    GlobalAddress addr;
    addr.nodeID = dsm_->getClusterSize() - 1;
    addr.offset = define::kRootPointerStoreOffest + sizeof(RootPtr) * tree_id_;
    return addr;
  }

  void set_new_root_ptr(GlobalAddress new_root_ptr) {
    assert(is_mine_ || dsm_->getMyNodeID() == 0);
    auto root_ptr_buffer = dsm_->get_rbuf(0).get_page_buffer();

    // GlobalAddress new_root_addr = allocate_node();
    RootPtr *root_ptr = new (root_ptr_buffer) RootPtr();
    root_ptr->rootptr = new_root_ptr;
    root_ptr->set_consistent();
    root_ptr_ = new_root_ptr;
    dsm_->write_sync(root_ptr_buffer, root_ptr_ptr_, sizeof(RootPtr));
    std::cout << "Success: tree root pointer value " << root_ptr_ << std::endl;
  }

  void get_new_root_ptr() {
    auto root_ptr_buffer = dsm_->get_rbuf(0).get_page_buffer();
    RootPtr *root_ptr = nullptr;
    bool retry = true;
    while (retry) {
      dsm_->read_sync(root_ptr_buffer, root_ptr_ptr_, sizeof(RootPtr));
      root_ptr = reinterpret_cast<RootPtr *>(root_ptr_buffer);
      retry = !root_ptr->check_consistent();
      std::cout << "Retry read root" << std::endl;
    }
    root_ptr_ = root_ptr->rootptr; 
    std::cout << "Read from mem tree root pointer value " << root_ptr_
              << std::endl;
  }

  // allocators
  GlobalAddress allocate_node() {
    auto node_addr = dsm_->alloc(kPageSize);
    return node_addr;
  }

  GlobalAddress allocate_node(uint32_t node_id) {
    auto node_addr = dsm_->alloc(kPageSize, dsm_->getClusterSize() - 1);
    return node_addr;
  }

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
};

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
//   static void batch_insert_leaf(BForest *forest, KVTS *kvs, int start, int
//   cnt,
//                                 int full_number, int thread_id,
//                                 CoroContext *cxt = nullptr, int coro_id = 0);
//   void batch_insert_internal(int thread_num);

//   GlobalAddress get_root_ptr_ptr(uint16_t id);
//   GlobalAddress get_root_ptr(CoroContext *cxt, int coro_id, uint16_t id);

//   void set_stack_buffer(int level, const Key &k, CoroContext *cxt, int
//   coro_id); void search_stack_buffer(int level, const Key &k, GlobalAddress
//   &result);

//   void split_leaf(KVTS *kvs, int start, int split_num, int range,
//                   BatchInsertFlag l_flag, BatchInsertFlag r_flag, int
//                   thread_id, CoroContext *cxt = nullptr, int coro_id = 0);
//   inline void go_in_leaf(BLeafPage *lp, int start, Key lowest, Key highest,
//                          int &next);
//   inline void go_in_kvts(KVTS *kvs, int start, int range, int &next);
//   void calculate_meta(int split_num, BatchInsertFlag l_flag);

//   inline void set_leaf(KVTS *kvs, const Key &k, const Value &v,
//                        bool &need_split, BatchInsertFlag l_flag,
//                        BatchInsertFlag r_flag, int pre_kv, int pro_kv,
//                        CoroContext *cxt = nullptr, int coro_id = 0);

//   void update_root(int level);

//   bool page_search(GlobalAddress page_addr, const Key &k, BSearchResult
//   &result,
//                    CoroContext *cxt, int coro_id);

//   void internal_page_search(BInternalPage *page, const Key &k,
//                             BSearchResult &result);

//   void leaf_page_search(BLeafPage *page, const Key &k, BSearchResult
//   &result);

//   bool check_ga(GlobalAddress ga);
// };
};  // namespace XMD
// namespace forest
