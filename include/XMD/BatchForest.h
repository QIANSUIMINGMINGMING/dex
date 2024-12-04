#pragma once

#include "../tree_api.h"
#include "BatchBtree.h"
#include "XMD_request_cache.h"
#include "mc_agent.h"
#include "request_cache_v2.h"

struct meta {
  std::atomic<int> num_of_key{0};
};

template <class T, class P>
class BatchForest : public tree_api<T, P> {
 public:
  BatchForest(DSM *dsm, uint64_t cache_mb, uint64_t request_mb, double sample,
              double admmision)
      // : my_dsm(dsm), request_cache_(request_mb * 1024 * 1024 /
      // XMD::kPageSize) {
      : my_dsm(dsm) {
    // batch_calculator = new libcuckoo::cuckoohash_map<GlobalAddress, struct meta*>();
    std::cout << "creating XMD" << std::endl;
    comp_node_num = dsm->getComputeNum();
    for (int i = 0; i < dsm->getComputeNum(); i++) {
      btrees[i] = new XMD::BatchBTree(dsm, i, cache_mb, sample, admmision);
    }
    if (dsm->getMyNodeID() == 0) {
      for (int i = 0; i < dsm->getComputeNum(); i++) {
        auto first_root = btrees[i]->allocate_node();
        btrees[i]->first_set_new_root_ptr(first_root);
      }
    }

    request_cache_ = new XMD::RequestCache_v3::RequestCache(
        dsm, XMD::defaultCacheSize, dsm->getMyNodeID(), dsm->getComputeNum());
    start_batch_insert();

    mcm = new XMD::multicast::multicastCM(dsm, 1);
    mcm->print_self();
    // mcm2 = std::make_unique<rdmacm::multicast::multicastCM>(dsm);

    dsm->barrier("init-mc", dsm->getComputeNum());

    tob = new XMD::multicast::TransferObjBuffer(mcm, 0);
    multicast_recv_thread =
        std::thread(XMD::multicast::TransferObjBuffer::fetch_thread_run, tob);

    dsm->barrier("set-roots1", dsm->getComputeNum());

    for (int i = 0; i < dsm->getComputeNum(); i++) {
      btrees[i]->first_get_new_root_ptr();
    }
  }

  // ~BatchForest() { stop_batch_insert(); }

  bool insert(T key, P value) {
    XMD::KVTS kvts = XMD::KVTS{key, value, XMD::myClock::get_ts()};
    request_cache_->insert(kvts);
    // if (rand() % comp_node_num == 0) {
    //   tob->packKVTS(kvts, 0);
    // }
    return true;
  }

  bool lookup(T key, P &value) {
    bool ret;
    TS ts_in_local;
    ret = request_cache_->rht_.find(key, ts_in_local, value);
    // bool ret = request_cache_->lookup(key, value);
    if (ret) {
      return ret;
    }
    // path (II)
    int shard_id = key % comp_node_num;
    // std::cout << "lookup" << key << std::endl;
    XMD::BatchBTree *shard_tree = btrees[shard_id];
    ret = shard_tree->search(key, value);
    // if (ret&& rand() % 2 ==0) {
    //   request_cache_->insert_no_TS(key, value);
    // }
    return ret;
  }

  bool update(T key, P value) {
    XMD::KVTS kvts = XMD::KVTS{key, value, XMD::myClock::get_ts()};
    int shard_id = key % comp_node_num;
    if (shard_id == my_dsm->getMyNodeID()) {
      XMD::BatchBTree *shard_tree = btrees[shard_id];
      GlobalAddress target_leaf;
      struct meta *this_meta;
      uint64_t taget_leaf_u64;
      shard_tree->partial_search(key, target_leaf);
      taget_leaf_u64 = target_leaf.val;
      if (!batch_calculator.contains(taget_leaf_u64)) {
        this_meta = new struct meta;
        batch_calculator.insert(taget_leaf_u64, this_meta);
        batch_leaf_num.fetch_add(1);
      } 
    }
    return true;
  }

  int clear_batch_info() {
    batch_calculator.clear();
    int ret = batch_leaf_num.load();
    batch_leaf_num.store(0);
    return ret;
  }

  void start_batch_insert() {
    batch_update_th = std::thread(
        XMD::RequestCache_v3::RequestCache::period_batch, request_cache_);
    batch_check_th = std::thread(
        XMD::RequestCache_v3::RequestCache::period_check, request_cache_);
  }

  void stop_batch_insert() {
    // batch_check_th.join();
    // batch_update_th.join();
    
  }

  // static void batch_insert_thread_run(XMD::BatchBTree *bbt, BatchForest *bf)
  // {
  //   bf->my_dsm->registerThread();

  //   while (true) {
  //     XMD::SkipList *cur_oldest = bf->request_cache_.get_oldest();
  //     bbt->batch_insert(cur_oldest);
  //     bf->request_cache_.update_oldest();
  //   }
  // }

  // void start_batch_insert() {
  //   batch_insert_thread = std::thread(batch_insert_thread_run,
  //   btrees[my_dsm->getMyNodeID()], this);
  // }

  // void stop_batch_insert() { batch_insert_thread.join(); }

  bool remove(T key) {
    XMD::KVTS kvts = XMD::KVTS{key, kValueNull, XMD::myClock::get_ts()};
    request_cache_->insert(kvts);
    // if (rand() % comp_node_num == 0) {
    //   tob->packKVTS(kvts, 0);
    // }
    return true;
  }

  int range_scan(T key, uint32_t num, std::pair<T, P> *&result) {
    int shard_id = key % my_dsm->getComputeNum();
    // std::cout << "lookup" << key << std::endl;
    XMD::BatchBTree *shard_tree = btrees[shard_id];
    return shard_tree->range_scan(key, num, result);
  }

  void clear_statistic() {}

  void bulk_load(T *bulk_array, uint64_t bulk_load_num) {
    uint32_t node_id = my_dsm->getMyNodeID();
    uint32_t compute_num = my_dsm->getComputeNum();
    if (node_id >= compute_num) {
      return;
    }
    XMD::BatchBTree *my_tree = btrees[node_id];
    for (uint64_t i = 0; i < bulk_load_num; i++) {
      T key = bulk_array[i];
      P value = bulk_array[i] + 1;
      if ((key % compute_num) == node_id) {
        my_tree->bulk_load_tree_->insert(key, value);
      }
      // std::cout << "i: "<<i<<"vv " << bulk_array[i] << std::endl;

      // my_tree->bulk_load_tree_->traverse();
    }
    assert(node_id == my_tree->tree_id_);
    my_tree->bulk_load_node_num = XMD::BTreeNode::node_count;
    // my_tree->bulk_load_tree_->traverse();

    std::cout << "my_tree node num: " << my_tree->bulk_load_node_num
              << std::endl;
    // ga_for_bulk = my_dsm->alloc(XMD::kPageSize *
    // my_tree->bulk_load_node_num);

    //     std::cout
    // << "allocated ga" << ga_for_bulk << std::endl;
    my_tree->bulk_loading();
    my_dsm->barrier("set-roots2", my_dsm->getComputeNum());

    for (int i = 0; i < my_dsm->getComputeNum(); i++) {
      btrees[i]->first_get_new_root_ptr();
      btrees[i]->single_thread_load_newest_root();
    }
  }

  // void reset_buffer_pool(bool flush_dirty) {
  //   cache.reset(flush_dirty);
  // }

  XMD::BatchBTree *btrees[MAX_MACHINE];
  // XMD::KVCache request_cache_;
  XMD::RequestCache_v3::RequestCache *request_cache_;

  // std::mutex request_cache_mutex_;
  // std::thread batch_insert_thread;
  std::thread batch_update_th;
  std::thread batch_check_th;
  XMD::multicast::multicastCM *mcm;
  XMD::multicast::TransferObjBuffer *tob;
  std::thread multicast_recv_thread;

  libcuckoo::cuckoohash_map<uint64_t, struct meta*> batch_calculator;
  std::atomic<int> batch_leaf_num{0};
  int comp_node_num;

  DSM *my_dsm;
  // Do most initialization work here
  tree_api<T, P> *create_tree() { return nullptr; }
};
