#pragma once

#include "BatchBtree.h"

template <class T, class P>
class BatchForest : public tree_api<T, P> {
 public:
  BatchForest(DSM *dsm, uint64_t cache_mb, double sample, double admmision) {
    std::cout << "creating XMD" << std::endl;
    my_dsm = dsm;
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

  bool update(T key, P value) { return true; }

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
    for (uint64_t i = 0; i < 200; i++) {
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
    my_tree->bulk_load_tree_->traverse();

    std::cout << "my_tree node num: " << my_tree->bulk_load_node_num
              << std::endl;
            // ga_for_bulk = my_dsm->alloc(XMD::kPageSize *
            // my_tree->bulk_load_node_num);

        //     std::cout
        // << "allocated ga" << ga_for_bulk << std::endl;
    my_tree->bulk_loading();



    // test
    // for (int i = 0; i < 10; i++) {
    //   XMD::BTreeNode *test_node = XMD::BTreeNode::first_ten[i];
    //   std::cout << "node id" << i << std::endl;
    //   for (int j = 0; j < test_node->numKeys; j++) {
    //     std::cout << test_node->keys[j] << " ";
    //   }
    //   std::cout << std::endl;

    //   GlobalAddress remote_addr;
    //   remote_addr.nodeID = ga_for_bulk.nodeID;
    //   remote_addr.offset = ga_for_bulk.offset + i * XMD::kPageSize;
    //   auto remote_page_buffer = my_dsm->get_rbuf(0).get_page_buffer();
    //   XMD::NodePage *remote_page_ptr;
    //   bool retry = true;
    //   while (retry) {
    //     my_dsm->read_sync(remote_page_buffer, remote_addr, XMD::kPageSize);
    //     remote_page_ptr = reinterpret_cast<XMD::NodePage
    //     *>(remote_page_buffer);
    //     // retry = false;
    //     retry = !(remote_page_ptr->check_consistent());
    //     std::cout << "Retry read page" << std::endl;
    //   }
    //   std::cout << "remote results ";
    //   for (int j = 0; j < test_node->numKeys; j++) {
    //     std::cout << remote_page_ptr->keys[j] << " ";
    //   }
    //   std::cout << std::endl;
    // }

    my_dsm->barrier("set-roots2", my_dsm->getComputeNum());

    for (int i = 0; i < my_dsm->getComputeNum(); i++) {
      btrees[i]->first_get_new_root_ptr();
      btrees[i]->single_thread_load_newest_root();
    }

    // set root

    Value v;

    lookup(1, v);

    std::cout << "value of 1" << v << std::endl;
    lookup(20000, v);
    std::cout << "value of 2" << v << std::endl;
    lookup(3000000, v);
    std::cout << "value of 3" << v << std::endl;

    while (true) {}
    // uint32_t compute_num = my_dsm->getComputeNum();
    // if (node_id >= compute_num) {
    //   return;
    // }
    // partition_info *all_partition = new partition_info[bulk_threads];
    // uint64_t each_partition = bulk_load_num / bulk_threads;

    // for (uint64_t i = 0; i < bulk_threads; ++i) {
    //   all_partition[i].id = i;
    //   all_partition[i].array =
    //       bulk_array + (all_partition[i].id * each_partition);
    //   all_partition[i].num = each_partition;
    // }
    // all_partition[bulk_threads - 1].num =
    //       bulk_load_num - (each_partition * (bulk_threads - 1));

    // auto bulk_thread = [&](void *bulk_info) {
    //   auto my_parition = reinterpret_cast<partition_info *>(bulk_info);
    //   // bindCore(my_parition->id);
    //   bindCore((my_parition->id % bulk_threads) * 2);
    //   my_dsm->registerThread();
    //   auto num = my_parition->num;
    //   auto array = my_parition->array;

    //   for (uint64_t i = 0; i < num; ++i) {
    //     T k = array[i];
    //     P v = array[i] + 1;
    //     if ((k % compute_num) == node_id)
    //     btrees[node_id]->bulk_load_insert_single(k, v);
    //   }
    // };

    // for (uint64_t i = 0; i < bulk_threads; i++) {
    //   th[i] =
    //       std::thread(bulk_thread, reinterpret_cast<void *>(all_partition +
    //       i));
    // }

    // for (uint64_t i = 0; i < bulk_threads; i++) {
    //   th[i].join();
    // }
  }

  XMD::BatchBTree *btrees[MAX_MACHINE];

  DSM *my_dsm;
  uint64_t bulk_threads = 8;
  std::thread th[8];
  // Do most initialization work here
  tree_api<T, P> *create_tree() { return nullptr; }
};
