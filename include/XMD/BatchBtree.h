#pragma once

#include <city.h>

// #include <boost/unordered/concurrent_flat_map.hpp>
// #include <boost/unordered/unordered_map.hpp>
#include <atomic>
#include <iostream>
#include <thread>

#include "XMD_index_cache.h"

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
               dsm->getMyNodeID() == tree_id ? true : false, &root_ptr_) {
    std::cout << "start generating tree " << tree_id << std::endl;
    super_root_ = new NodePage(255, GlobalAddress::Null());
    super_root_->header.pos_state = 2;
    dsm_ = dsm;
    tree_id_ = tree_id;
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
      if (needRestart) {
        if (refresh) {
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
          cur_node->header.bitmap = 2;
          cur_node->writeUnlock();
          inner->IOUnlock();
        } else {
          inner->IOUnlock();
          if (refresh) {
            new_refresh_from_root(k);
          }
          goto restart;
        }
      }

      // Get the version of next node
      versionNode = cur_node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
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

  void batch_insert() { assert(is_mine_); }

  void bulk_load_insert_single(const Key &k, const Value &v) {}

  DSM *dsm_;
  // GlobalAddress root_;  // can be swizzled
  NodePage *super_root_;
  GlobalAddress root_ptr_ptr_;
  GlobalAddress root_ptr_;

  int height_ = 0;

  BTree *bulk_load_tree_;
  int bulk_load_node_num = 0;

  uint64_t tree_id_;
  bool is_mine_;

  CacheManager cache_;

  void bulk_loading(const GlobalAddress &mem_base) {
    BTreeNode *bulk_tree_root = bulk_load_tree_->root;
    GlobalAddress root_addr = bulk_tree_root->to_remote(
        dsm_, mem_base, bulk_load_tree_->height,
        std::numeric_limits<Key>::min(), std::numeric_limits<Key>::max());

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

    super_root_->values[0] = root_ptr_.val;
    if (get_memory_address(root_ptr_) != nullptr) {
      super_root_->header.set_bitmap(0);
    }
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
    std::cout << "dsm_ address" <<dsm_ <<std::endl;
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
      auto mem_root = reinterpret_cast<NodePage *>(get_memory_address(root_ptr_));
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

  void new_refresh_from_root(Key k) { return; }
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
