#pragma once

#include "BatchBtree.h"

template <class T, class P>
class BatchForest : public tree_api<T, P> {
 public:
  BatchForest(DSM *dsm) {
    my_dsm = dsm;
    for (int i = 0; i < dsm->getComputeNum();i++) {
        btrees[i] = new XMD::BatchBTree(dsm, i);
    }    
    if (dsm->getMyNodeID() == 0) {
        for (int i = 0; i < dsm->getComputeNum(); i++) {
            auto first_root = btrees[i]->allocate_node();
            btrees[i]->set_new_root_ptr(first_root);
        }
    }
    dsm->barrier("set roots", dsm->getComputeNum());

    for (int i = 0; i< dsm->getComputeNum();i++) {
        btrees[i]->get_new_root_ptr();
    }
  }

  bool insert(T key, P value) {

  }

  bool lookup(T key, P &value) {

  }

  bool update(T key, P value) {

  }

  bool remove(T key) {

  }

  int range_scan(T key, uint32_t num, std::pair<T, P> *&result) {

  }

  void clear_statistic() {} 

  void bulk_load(T *bulk_array, uint64_t bulk_load_num) {
    uint32_t node_id = my_dsm->getMyNodeID();
    uint32_t compute_num = my_dsm->getComputeNum();
    if (node_id >= compute_num) {
      return;
    }
    XMD::BatchBTree * my_tree = btrees[node_id];
    for (uint64_t i = 0; i < bulk_load_num; i++) {
      T key = bulk_array[i];
      P value = bulk_array[i] + 1;
      if ((key % compute_num) == node_id) {
        my_tree->bulk_load_tree_->insert(key, value);
      }
      std::cout << "i: " << i << std::endl;
    }

    std::cout << "my_tree node num: " << my_tree->bulk_load_node_num << std::endl;

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
    //       std::thread(bulk_thread, reinterpret_cast<void *>(all_partition + i));
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
