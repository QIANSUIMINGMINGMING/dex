#pragma once

#include "BatchBtree.h"
#include "XMD_request_cache.h"
#include "mc_agent.h"

template <class T, class P>
class BatchForest : public tree_api<T, P> {
 public:
  BatchForest(DSM *dsm, uint64_t cache_mb, uint64_t request_mb, double sample,
              double admmision)
      : my_dsm(dsm), request_cache_(request_mb * 1024 * 1024 / XMD::kPageSize) {
    std::cout << "creating XMD" << std::endl;
    for (int i = 0; i < dsm->getComputeNum(); i++) {
      btrees[i] = new XMD::BatchBTree(dsm, i, cache_mb, sample, admmision);
    }
    if (dsm->getMyNodeID() == 0) {
      for (int i = 0; i < dsm->getComputeNum(); i++) {
        auto first_root = btrees[i]->allocate_node();
        btrees[i]->first_set_new_root_ptr(first_root);
      }
    }
    dsm->barrier("set-roots1", dsm->getComputeNum());

    for (int i = 0; i < dsm->getComputeNum(); i++) {
      btrees[i]->first_get_new_root_ptr();
    }
  }

  bool insert(T key, P value) { return true; }

  bool lookup(T key, P &value) {
    int shard_id = key % my_dsm->getComputeNum();
    // std::cout << "lookup" << key << std::endl;
    XMD::BatchBTree *shard_tree = btrees[shard_id];
    return shard_tree->search(key, value);
  }

  bool update(T key, P value) {
    XMD::KVTS kvts[my_dsm->getComputeNum()];
    for (int i = 0; i < my_dsm->getComputeNum(); i++) {
      kvts[i] = XMD::KVTS{key + i, value, XMD::myClock::get_ts()};
    }

    std::lock_guard<std::mutex> lock(request_cache_mutex_);

    for (int i = 0; i < my_dsm->getComputeNum(); i++) {
      request_cache_.insert(kvts[i]);
    }

    return true;
  }

  static void batch_insert_thread_run(XMD::BatchBTree *bbt, BatchForest *bf) {
    bf->my_dsm->registerThread();

    while (true) {
      XMD::SkipList *cur_oldest = bf->request_cache_.get_oldest();
      bbt->batch_insert(cur_oldest);
      bf->request_cache_.update_oldest();
    }
  }

  void start_batch_insert() {
    batch_insert_thread = std::thread(batch_insert_thread_run, btrees[my_dsm->getMyNodeID()], this);
  }

  void stop_batch_insert() { batch_insert_thread.join(); }

  bool remove(T key) { return true; }

  int range_scan(T key, uint32_t num, std::pair<T, P> *&result) { return 0; }

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
  XMD::KVCache request_cache_;
  std::mutex request_cache_mutex_;
  std::thread batch_insert_thread;

  DSM *my_dsm;
  // Do most initialization work here
  tree_api<T, P> *create_tree() { return nullptr; }
};
