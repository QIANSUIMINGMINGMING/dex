#pragma once

#include "BatchBtree.h"

namespace XMD {
template <class T, class P>
class BatchForest : public tree_api<T, P> {
 public:
  BatchForest(DSM *dsm) {
    my_dsm = dsm;
    for (int i = 0; i < dsm->getClusterSize();i++) {
        btrees[i] = new BatchBTree(dsm, i);
    }    
    if (dsm->getMyNodeID() == 0) {
        for (int i = 0; i < dsm->getClusterSize(); i++) {
            auto first_root = btrees[i]->allocate_node();
            btrees[i]->set_new_root_ptr(first_root);
        }
    }
    dsm->barrier("set roots");

    for (int i = 0; i< dsm->getClusterSize();i++) {
        btrees[i]->get_new_root_ptr();
    }
  }

  bool insert(T key, P value) {}

  bool lookup(T key, P &value) {}

  bool update(T key, P value) {}

  bool remove(T key) {}

  int range_scan(T key, uint32_t num, std::pair<T, P> *&result) {}

  void clear_statistic() { }

  void bulk_load(T *bulk_array, uint64_t bulk_load_num) {}

  BatchBTree *btrees[MAX_MACHINE];
  DSM *my_dsm;
  uint64_t bulk_threads = 8;
  std::thread th[8];
  // Do most initialization work here
  tree_api<T, P> *create_tree() { return nullptr; }
};
}  // namespace XMD
