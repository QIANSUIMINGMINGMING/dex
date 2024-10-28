#pragma once

#include <city.h>

// #include <boost/unordered/concurrent_flat_map.hpp>
// #include <boost/unordered/unordered_map.hpp>
#include <atomic>
#include <iostream>
#include <libcuckoo/cuckoohash_map.hh>
#include <thread>

#include "XMD_index_cache.h"
#include "XMD_request_cache.h"

namespace XMD {

// crc kv
// constexpr int kInternalCardinality =
//     (kPageSize - sizeof(uint32_t) - sizeof(uint8_t) - sizeof(uint8_t) -
//      2 * sizeof(bool)) /
//     (sizeof(Key) + sizeof(GlobalAddress));

// constexpr int kLeafCardinality =
//     (kPageSize - sizeof(uint32_t) - sizeof(uint8_t) - sizeof(uint8_t) -
//      2 * sizeof(bool)) /
//         sizeof(Key) +
//     sizeof(Value);

constexpr int kLeafCardinality = kInternalCardinality;
constexpr int one_batch_nodes = 64;

// For bulkloading

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
  BatchBTree(DSM *dsm, uint64_t tree_id, uint64_t cache_mb = 1,
             double sample_rate = 0.1, double admission_rate = 0.1)
      : cache_(cache_mb * 1024 * 1024 / kPageSize, sample_rate, admission_rate,
               &root_ptr_) {
    std::cout << "start generating tree " << tree_id << std::endl;
    super_root_ = new NodePage(255, GlobalAddress::Null());
    super_root_->header.pos_state = 2;
    dsm_ = dsm;
    tree_id_ = tree_id;
    cnode_num_ = dsm->getComputeNum();
    is_mine_ = dsm_->getMyNodeID() == tree_id ? true : false;
    root_ptr_ptr_ = get_root_ptr_ptr();
    bulk_load_tree_ = new BTree(keyNum / 2 + 1);
  }

  bool search(Key k, Value &result) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    NodePage *parent = super_root_;
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    NodePage *cur_node = cache_.search_in_cache(root_ptr_);
    if (cur_node == nullptr) {
      assert(false);
      // Load newest root
      load_newest_root(versionParent, needRestart);
      goto restart;
    }

    // printNodePage(*cur_node);

    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    while (!cur_node->is_leaf) {
      auto inner = cur_node;
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }

      parent = inner;
      versionParent = versionNode;
      // Got the next child to go
      auto idx = inner->lowerBound(k);
      assert(idx != -1);
      GlobalAddress inner_child_ga;
      if (idx == LeftMostIdx) {
        cur_node = new_get_mem_node(inner->left_ptr, inner, idx, versionNode,
                                    needRestart, refresh, false);
        inner_child_ga = inner->left_ptr;
      } else {
        cur_node = new_get_mem_node(
            *(reinterpret_cast<GlobalAddress *>(&inner->values[idx])), inner,
            idx, versionNode, needRestart, refresh, false);

        inner_child_ga.val = inner->values[idx];
      }

      if (inner_child_ga.val == 0) {
        printNodePage(*inner);
        assert(false);
      }

      if (needRestart) {
        if (refresh) {
          assert(false);
          new_refresh_from_root(k);
        }
        goto restart;
      }

      // IO flag has been inserted into the page table
      // But the operation is not finished
      if (cur_node == nullptr) {
        int remote_flag = 0;
        if (inner->header.level == 1) {
          // Admission control is needed
          bool lookup_success = false;
          remote_flag = cache_.cold_to_hot_with_admission(
              inner_child_ga, reinterpret_cast<void **>(&cur_node), inner, idx,
              refresh, k, result, lookup_success, cachepush::RPC_type::LOOKUP);
          if (remote_flag == 1) {
            inner->IOUnlock();
            return lookup_success;
          }
        }
        //  else if (inner->header.level <= megaLevel) {
        //   // RPC is probably needed
        //   bool lookup_success = false;
        //   remote_flag = cache_.cold_to_hot_with_rpc(
        //       inner->children[idx], reinterpret_cast<void **>(&cur_node),
        //       inner, idx, refresh, k, result, lookup_success,
        //       RPC_type::LOOKUP);
        //   if (remote_flag == 1) {
        //     inner->IOUnlock();
        //     return lookup_success;
        //   }
        // }
        else {
          // assert(false);
          // Upper than subtree; then directly load from remote
          remote_flag = cache_.cold_to_hot(inner_child_ga,
                                           reinterpret_cast<void **>(&cur_node),
                                           inner, idx, refresh);
        }

        // Admission succeed
        if (remote_flag == 0) {
          assert(cur_node != nullptr);
          if (idx == LeftMostIdx) {
            new_swizzling(inner->left_ptr, inner, idx, cur_node);
          } else {
            new_swizzling(
                *(reinterpret_cast<GlobalAddress *>(&inner->values[idx])),
                inner, idx, cur_node);
          }
          cur_node->header.pos_state = 2;
          cur_node->writeUnlock();
          inner->IOUnlock();
        } else {
          inner->IOUnlock();
          if (refresh) {
            assert(false);
            new_refresh_from_root(k);
          }
          goto restart;
        }
      }

      // Get the version of next node
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;

      // printNodePage(*cur_node);
    }

    // Access the leaf node
    NodePage *leaf = cur_node;
    if (!leaf->header.rangeValid(k)) {
      new_refresh_from_root(k);
      goto restart;
    }

    // assert(leaf->rangeValid(k) == true);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->header.count) && (pos != LeftMostIdx) &&
        (leaf->keys[pos] == k)) {
      success = true;
      result = leaf->values[pos];
    } else {
      result = std::numeric_limits<Value>::max();
    }

    auto backup_min = leaf->header.min_limit_;
    auto backup_max = leaf->header.max_limit_;

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }
    }

    cur_node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    assert(rangeValid(k, backup_min, backup_max));
    return success;
    return true;
  }

  void batch_insert(SkipList *skiplist) {
    assert(is_mine_);
    phaseI(skiplist);
    phaseII();
    phaseIII();
  }

  DSM *dsm_;
  // GlobalAddress root_;  // can be swizzled
  NodePage *super_root_;
  GlobalAddress root_ptr_ptr_;
  GlobalAddress root_ptr_;

  struct hash_value {
    std::mutex mtx;
    std::vector<KVTS *> kvtss;
    hash_value() {}
    void insert(KVTS *kvts) {
      mtx.lock();
      kvtss.push_back(kvts);
      mtx.unlock();
    }
  };

  struct meta_value {
    std::atomic<int> kids{0};
    GlobalAddress parent_remote_addr;
    int idx_in_parent;
  };

  // local memory
  libcuckoo::cuckoohash_map<NodePage *, hash_value *> nodeUpdatingTable;

  libcuckoo::cuckoohash_map<NodePage *, hash_value *> innerNodeUpdatingTable;
  // remote memory
  libcuckoo::cuckoohash_map<uint64_t, meta_value *> TreeMetatable;

  int height_ = 0;

  BTree *bulk_load_tree_;
  int bulk_load_node_num = 0;

  uint64_t tree_id_;
  uint64_t cnode_num_;
  bool is_mine_;

  CacheManager cache_;

  bool partial_search(Key k, NodePage *&result) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    NodePage *parent = super_root_;
    bool needRestart = false;
    bool refresh = false;
    uint64_t versionParent = super_root_->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    NodePage *cur_node = cache_.search_in_cache(root_ptr_);
    if (cur_node == nullptr) {
      assert(false);
      // Load newest root
      load_newest_root(versionParent, needRestart);
      goto restart;
    }
    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    while (!cur_node->is_leaf) {
      auto inner = cur_node;
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }

      parent = inner;
      versionParent = versionNode;
      // Got the next child to go
      auto idx = inner->lowerBound(k);
      assert(idx != -1);
      GlobalAddress inner_child_ga;
      if (idx == LeftMostIdx) {
        cur_node = new_get_mem_node(inner->left_ptr, inner, idx, versionNode,
                                    needRestart, refresh, false);
        inner_child_ga = inner->left_ptr;
      } else {
        cur_node = new_get_mem_node(
            *(reinterpret_cast<GlobalAddress *>(&inner->values[idx])), inner,
            idx, versionNode, needRestart, refresh, false);

        inner_child_ga.val = inner->values[idx];
      }

      if (inner_child_ga.val == 0) {
        printNodePage(*inner);
        assert(false);
      }

      if (needRestart) {
        if (refresh) {
          assert(false);
          new_refresh_from_root(k);
        }
        goto restart;
      }

      // IO flag has been inserted into the page table
      // But the operation is not finished
      if (cur_node == nullptr) {
        if (inner->header.level == 1) {
        } else {
          int remote_flag = 0;

          remote_flag = cache_.cold_to_hot(inner_child_ga,
                                           reinterpret_cast<void **>(&cur_node),
                                           inner, idx, refresh);

          // Admission succeed
          if (remote_flag == 0) {
            assert(cur_node != nullptr);
            if (idx == LeftMostIdx) {
              new_swizzling(inner->left_ptr, inner, idx, cur_node);
            } else {
              new_swizzling(
                  *(reinterpret_cast<GlobalAddress *>(&inner->values[idx])),
                  inner, idx, cur_node);
            }
            cur_node->header.pos_state = 4;
            cur_node->writeUnlock();
            inner->IOUnlock();
          } else {
            inner->IOUnlock();
            if (refresh) {
              assert(false);
              new_refresh_from_root(k);
            }
            goto restart;
          }
        }
      }

      // Get the version of next node
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;

      // printNodePage(*cur_node);
    }

    // Access the leaf node
    NodePage *leaf = cur_node;
    if (!leaf->header.rangeValid(k)) {
      assert(false);
      new_refresh_from_root(k);
      goto restart;
    }

    result = leaf;

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }
    }

    cur_node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }
    return true;
  }

  void phaseI(SkipList *skpl) {
    SkipListNode *cur = skpl->getHead();
    NodePage *cur_leaf = nullptr;
    hash_value *next_hash = new hash_value();
    hash_value *cur_hash = nullptr;
    for (uint64_t i = 0; i < kIntervalSize; i++) {
      cur = skpl->getNext(cur);
      KVTS &cur_kvts = cur->kvts;
      Key cur_k = cur_kvts.k;
      if (cur_kvts.k % cnode_num_ == tree_id_) {
        if (cur_leaf == nullptr || (cur_leaf->header.min_limit_ > cur_k &&
                                    cur_leaf->header.max_limit_ <= cur_k)) {
          partial_search(cur_k, cur_leaf);
          assert(cur_leaf->is_leaf);
          if (!nodeUpdatingTable.contains(cur_leaf)) {
            nodeUpdatingTable.insert(cur_leaf, next_hash);
            next_hash->insert(&cur_kvts);
            cur_hash = next_hash;
            next_hash = new hash_value();
          } else {
            hash_value *hv = nodeUpdatingTable.find(cur_leaf);
            hv->insert(&cur_kvts);
            cur_hash = hv;
          }
        } else {
          cur_hash->insert(&cur_kvts);
        }
      }
    }
  }

  void leaf_to_remote(
      std::vector<NodePage *> splitted_siblings) {
    int total_idx = 0;
    int next_batch_num = splitted_siblings.size() > one_batch_nodes
                             ? one_batch_nodes
                             : splitted_siblings.size();
    GlobalAddress next_batch_addr;
    if (next_batch_num > 0) {
      next_batch_addr = dsm_->alloc(next_batch_num * kPageSize);
    }
    while (total_idx < splitted_siblings.size()) {
      int cur_batch_num = next_batch_num;
      auto batch_buffer = dsm_->get_rbuf(0).get_batch_page_buffer();
      for (int i = 0; i < cur_batch_num; i++) {
        NodePage *cur_leaf = splitted_siblings[total_idx + i];
        GlobalAddress cur_remote_addr = next_batch_addr + i * kPageSize;
        cur_leaf->header.remote_address = cur_remote_addr;
        cur_leaf->set_consistent();
        memcpy(batch_buffer + i * kPageSize, cur_leaf, kPageSize);
      }
      dsm_->write_sync(batch_buffer, next_batch_addr, cur_batch_num * kPageSize);
      total_idx += cur_batch_num;
    }

    // update original leaf node
    auto lt = nodeUpdatingTable.lock_table();
    for (const auto &it : lt) {
      NodePage *cur_leaf = it.first;
      cur_leaf->set_consistent();
      GlobalAddress cur_leaf_addr = cur_leaf->header.remote_address;
      auto dsm_buffer = dsm_->get_rbuf(0).get_page_buffer();
      memcpy(dsm_buffer, cur_leaf, kPageSize);
      dsm_->write_sync(dsm_buffer, cur_leaf_addr, kPageSize);
      cur_leaf->header.pos_state = 2;
    }
    lt.unlock();
    nodeUpdatingTable.clear();
  }

  void phaseII() {
    auto lt = nodeUpdatingTable.lock_table();
    std::vector<NodePage *> new_page_nodes;
    for (const auto &it : lt) {
      NodePage *cur_leaf = it.first;
      hash_value *hv = it.second;
      if (hv->kvtss.size() == 0) {
        assert(false);
      }
      bool needRestart = false;
      cur_leaf->header.writeLockOrRestart(needRestart);
      if (needRestart) {
        assert(false);
      }
      cur_leaf->updateNode(hv->kvtss, new_page_nodes);
      cur_leaf->header.writeUnlock();
    }
    lt.unlock();

    leaf_to_remote(new_page_nodes);
    nodeUpdatingTable.clear();

    NodePage *parent = nullptr;
    for (auto &node : new_page_nodes) {
      parent = node->parent_ptr;
      if (innerNodeUpdatingTable.contains(parent)) {
        hash_value *hv = innerNodeUpdatingTable.find(parent);
        hv->mtx.lock();
        hv->kvtss.push_back(
            new KVTS(node->keys[0], node->header.remote_address, 0));
        hv->mtx.unlock();
      } else {
        hash_value *hv = new hash_value();
        hv->kvtss.push_back(
            new KVTS(node->keys[0], node->header.remote_address, 0));
        innerNodeUpdatingTable.insert(parent, hv);
      }
    }
  }

  void phaseIII() {
    //TODO
    // auto lt = innerNodeUpdatingTable.lock_table();
    // std::vector<NodePage *> new_page_nodes;
    // for (const auto &it : lt) {
    //   NodePage *cur_inner = it.first;
    //   hash_value *hv = it.second;
    //   if (hv->kvtss.size() == 0) {
    //     assert(false);
    //   }
    //   bool needRestart = false;
    //   cur_inner->header.writeLockOrRestart(needRestart);
    //   if (needRestart) {
    //     assert(false);
    //   }
    //   cur_inner->updateNode(hv->kvtss, new_page_nodes);
    //   cur_inner->header.writeUnlock();
    // }
    // lt.unlock();

    // leaf_to_remote(new_page_nodes);
    // innerNodeUpdatingTable.clear();
  }

  void bulk_loading() {
    BTreeNode *bulk_tree_root = bulk_load_tree_->root;
    GlobalAddress root_addr = bulk_tree_root->to_remote(
        dsm_, bulk_load_tree_->height, std::numeric_limits<Key>::min(),
        std::numeric_limits<Key>::max());

    std::cout << "my_bulk_tree address: " << root_addr << std::endl;

    first_set_new_root_ptr(root_addr);
  }

  GlobalAddress get_root_ptr_ptr() {
    GlobalAddress addr;
    addr.nodeID = dsm_->getClusterSize() - 1;
    addr.offset = define::kRootPointerStoreOffest + sizeof(RootPtr) * tree_id_;
    return addr;
  }

  void first_set_new_root_ptr(GlobalAddress new_root_ptr) {
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

  void first_get_new_root_ptr() {
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

  GlobalAddress get_new_root_ptr() {
    auto root_ptr_buffer = dsm_->get_rbuf(0).get_page_buffer();
    // std::cout << "dsm_ address" <<dsm_ <<std::endl;
    RootPtr *root_ptr = nullptr;
    bool retry = true;
    while (retry) {
      dsm_->read_sync(root_ptr_buffer, root_ptr_ptr_, sizeof(RootPtr));
      root_ptr = reinterpret_cast<RootPtr *>(root_ptr_buffer);
      retry = !root_ptr->check_consistent();
    }
    root_ptr_ = root_ptr->rootptr;
    return root_ptr_;
  }

  void load_newest_root(uint64_t versionNode, bool &needRestart) {
    auto remote_root_ptr = get_new_root_ptr();
    auto cur_root_ptr = get_global_address(root_ptr_);

    if ((remote_root_ptr.val != cur_root_ptr.val) ||
        (cache_.search_in_cache(root_ptr_) == nullptr)) {
      auto remote_root = checked_remote_read(remote_root_ptr);
      if (remote_root == nullptr) return;

      super_root_->upgradeToWriteLockOrRestart(versionNode, needRestart);
      if (needRestart) {
        return;
      }

      needRestart = true;
      assert(cur_root_ptr == get_global_address(root_ptr_));
      // Load the lock from remote to see whether it is locked

      cache_.cache_insert(remote_root_ptr, remote_root, super_root_);
      auto old_mem_root =
          reinterpret_cast<NodePage *>(get_memory_address(root_ptr_));
      if (old_mem_root != nullptr) {
        old_mem_root->parent_ptr = nullptr;
      }

      root_ptr_ = remote_root_ptr;
      auto mem_root =
          reinterpret_cast<NodePage *>(get_memory_address(root_ptr_));
      assert(mem_root != nullptr);
      mem_root->parent_ptr = super_root_;
      super_root_->values[0] = root_ptr_;
      super_root_->header.set_bitmap(0);
      height_ = mem_root->header.level + 1;
      std::cout << "Fetched new height = " << height_ << std::endl;
      mem_root->header.pos_state = 2;
      mem_root->writeUnlock();
      super_root_->writeUnlock();
      return;
    }
  }

  void single_thread_load_newest_root() {
    assert(cache_.search_in_cache(root_ptr_) == nullptr);
    auto remote_root = checked_remote_read(root_ptr_);
    assert(remote_root != nullptr);
    std::cout << "tree id " << tree_id_ << " level "
              << (unsigned)remote_root->header.level << "key num "
              << (unsigned)remote_root->header.count;
    assert(remote_root->header.level == bulk_load_tree_->height || !is_mine_);
    cache_.cache_insert(root_ptr_, remote_root, super_root_);

    auto mem_root = reinterpret_cast<NodePage *>(get_memory_address(root_ptr_));
    assert(cache_.search_in_cache(root_ptr_) != nullptr);
    mem_root->parent_ptr = super_root_;
    super_root_->values[0] = root_ptr_;
    super_root_->header.set_bitmap(0);
    height_ = mem_root->header.level + 1;
    std::cout << "Fetched new height = " << height_ << std::endl;
    // print_node

    mem_root->header.pos_state = 2;
    mem_root->writeUnlock();
    printNodePage(*mem_root);

    return;
  }

  NodePage *new_get_mem_node(GlobalAddress &global_addr, NodePage *parent,
                             unsigned child_idx, uint64_t versionNode,
                             bool &restart, bool &refresh, bool IO_enable) {
    GlobalAddress node = global_addr;
    if (node.val & swizzle_tag) {
      return reinterpret_cast<NodePage *>(node.val & swizzle_hide);
    }

    cache_.opportunistic_sample();
    parent->upgradeToIOLockOrRestart(versionNode, restart);
    if (restart) {
      return nullptr;
    }

    GlobalAddress snapshot = global_addr;
    auto page = get_memory_address(snapshot);
    if (page) {
      parent->IOUnlock();
      return reinterpret_cast<NodePage *>(page);
    }

    auto target_node =
        cache_.cache_get(node, parent, child_idx, restart, refresh, IO_enable);
    if (target_node != nullptr) {
      new_swizzling(global_addr, parent, child_idx, target_node);
      target_node->header.pos_state = 2;
      target_node->writeUnlock();
    }

    // If IO is not supported, and IO flag is successfully inserteed
    if (!restart && target_node == nullptr) {
      assert(IO_enable == false);
      return target_node;
    }

    parent->IOUnlock();
    return target_node;
  }

  void new_refresh_from_root(Key k) {
    assert(false);
    // refresh until we entered into the non-shared node (e.g., leaf)
    // A solution: all new nodes can be treated as shared node!
    int restartCount = 0;
    bool verbose = false;
  restart:
    if (restartCount++) yield(restartCount);

    bool needRestart = false;
    bool refresh = false;
    NodePage *cur_node = super_root_;
    uint64_t versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    load_newest_root(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    NodePage *parent = reinterpret_cast<NodePage *>(cur_node);
    uint64_t versionParent = versionNode;
    cur_node = cache_.search_in_cache(root_ptr_);
    assert(cur_node != nullptr);

    versionNode = cur_node->readLockOrRestart(needRestart);
    if (needRestart) {
      goto restart;
    }

    // && cur_node->isShared()
    while (!cur_node->is_leaf) {
      auto inner = reinterpret_cast<NodePage *>(cur_node);
      // Compare local with remote node
      parent_read_unlock(parent, versionParent, needRestart);
      if (needRestart) goto restart;

      parent = inner;
      versionParent = versionNode;
      auto idx = inner->lowerBound(k);
      if (idx == LeftMostIdx) {
        cur_node = new_get_mem_node(inner->left_ptr, parent, idx, versionParent,
                                    needRestart, refresh, true);
      } else {
        cur_node = new_get_mem_node(
            *reinterpret_cast<GlobalAddress *>(&inner->values[idx]), parent,
            idx, versionParent, needRestart, refresh, true);
      }

      if (needRestart) {
        goto restart;
      }

      assert(cur_node != nullptr);
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart) {
        goto restart;
      }
    }
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

  void yield(int count) {
    if (count > 3)
      sched_yield();
    else
      _mm_pause();
  }

  int get_node_ID(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      auto mem_node = reinterpret_cast<NodePage *>(node & swizzle_hide);
      return mem_node->header.remote_address.nodeID;
    }
    return global_node.nodeID;
  }

  GlobalAddress get_global_address(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      auto mem_node = reinterpret_cast<NodePage *>(node & swizzle_hide);
      return mem_node->header.remote_address;
    }
    return global_node;
  }

  void *get_memory_address(GlobalAddress global_node) {
    uint64_t node = global_node.val;
    if (node & swizzle_tag) {
      return reinterpret_cast<void *>(node & swizzle_hide);
    }
    return nullptr;
  }

  void parent_read_unlock(NodePage *parent, uint64_t versionParent,
                          bool &needRestart) {
    if (parent == super_root_) {
      // Super root
      auto cur_root = get_global_address(root_ptr_);
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        return;
      }
    } else {
      // Normal parent
      // Local check
      auto parent_remote_addr = parent->header.remote_address;
      auto parent_version = parent->front_version;
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        return;
      }
    }
  }

  bool rangeValid(Key k, Key cur_min, Key cur_max) {
    if (std::numeric_limits<Key>::min() == k && k == cur_min) return true;
    if (k < cur_min || k >= cur_max) return false;
    return true;
  }

  void new_swizzling(GlobalAddress &global_addr, NodePage *parent,
                     unsigned child_idx, NodePage *child) {
    if (!check_parent_child_info(parent, child)) {
      std::cout << "Gloobal addr = " << global_addr << std::endl;
      auto new_child = raw_remote_read(child->header.remote_address);
      assert(check_parent_child_info(parent, child));
    }
    child->parent_ptr = parent;
    child->header.pos_state = 2;
    global_addr.val = reinterpret_cast<uint64_t>(child) | swizzle_tag;
    auto inner_parent = reinterpret_cast<NodePage *>(parent);
    inner_parent->header.set_bitmap(child_idx);
    assert(inner_parent->header.level != 255);
    assert(check_parent_child_info(parent, child));
  }
};

};  // namespace XMD
// namespace forest
