#include <immintrin.h>
#include <sched.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <list>
#include <unordered_map>
#include <utility>

#include "../DSM.h"
#include "../GlobalAddress.h"
#include "../cache/btree_node.h"

namespace XMD {
using Key = uint64_t;
using Value = uint64_t;

constexpr int LeftMostIdx = 63;
constexpr int kPageSize = 1024;

static const uint64_t swizzle_tag = 1ULL << 63;
static const uint64_t swizzle_hide = (1ULL << 63) - 1;

class NodeHeader : public cachepush::OptLock {
 public:
  GlobalAddress remote_address;
  uint64_t bitmap;  // 32B
  uint8_t count;
  uint8_t level;      // 0 means leaf, 1 means inner node untop of leaf nodes
  uint8_t pos_state;  // 0 means in remote memory, 1 means in cooling state, 2
                      // means in hot state, 3 means in local working set
                      // (e.g., after delete or evict) 40B
                      // 4 means pinnning => cannot be sampled...
  bool obsolete;      // obsolete means this page is obsolete

  // Actually level and type could be merged
  // used for concurrent sync in node accesses
  // (min_limit_, max_limit_]
  Key min_limit_;
  Key max_limit_;  // 64B

  NodeHeader(uint8_t cur_level, GlobalAddress remote) {
    level = cur_level;
    remote_address = remote;
    count = 0;
    obsolete = false;
    pos_state = 0;
    bitmap = 0;
    // lock = 0;
    min_limit_ = std::numeric_limits<Key>::min();
    max_limit_ = std::numeric_limits<Key>::max();
  }

  bool check_obsolete() { return obsolete; }

  void set_bitmap(int idx) { bitmap = bitmap | (1ULL << idx); }

  void unset_bitmap(int idx) { bitmap = bitmap & (~(1ULL << idx)); }

  bool test_bimap(int idx) { return bitmap & (1ULL << idx); }

  int closest_set(int pos) const {
    if (bitmap == 0) return -1;

    int bit_pos = std::min(pos, 63);
    uint64_t bitmap_data = bitmap;
    int closest_right_gap_distance = 64;
    int closest_left_gap_distance = 64;
    // Logically sets to the right of pos, in the bitmap these are sets to the
    // left of pos's bit
    // This covers the case where pos is a 1
    // cover idx: [pos, 63]
    uint64_t bitmap_right_sets = bitmap_data & (~((1ULL << bit_pos) - 1));
    if (bitmap_right_sets != 0) {
      closest_right_gap_distance =
          static_cast<int>(_tzcnt_u64(bitmap_right_sets)) - bit_pos;
    }

    // Logically sets to the left of pos, in the bitmap these are sets to the
    // right of pos's bit
    // cover idx: [0, pos - 1]
    uint64_t bitmap_left_sets = bitmap_data & ((1ULL << bit_pos) - 1);
    if (bitmap_left_sets != 0) {
      closest_left_gap_distance =
          bit_pos - (63 - static_cast<int>(_lzcnt_u64(bitmap_left_sets)));
    }

    if (closest_right_gap_distance < closest_left_gap_distance &&
        pos + closest_right_gap_distance < (count + 1)) {
      return pos + closest_right_gap_distance;
    } else {
      return pos - closest_left_gap_distance;
    }
  }

  bool rangeValid(Key k) {
    if (std::numeric_limits<Key>::min() == k && k == min_limit_) return true;
    if (k < min_limit_ || k >= max_limit_) return false;
    return true;
  }
};

constexpr int kInternalCardinality =
    (kPageSize - sizeof(uint32_t) - sizeof(uint8_t) - sizeof(uint8_t) -
     sizeof(bool) - sizeof(GlobalAddress) - sizeof(uint64_t) -
     sizeof(NodeHeader)) /
    (sizeof(Key) + sizeof(GlobalAddress));

class NodePage {
 public:
  uint32_t crc = 0;
  uint8_t front_version = 0;
  NodeHeader header;
  NodePage* parent_ptr;  // The ptr to the parent;
  GlobalAddress left_ptr = GlobalAddress::Null();
  Value values[kInternalCardinality]{0};
  Key keys[kInternalCardinality]{0};
  bool is_leaf;
  // for bulk loading
  uint8_t rear_version = 0;
  uint64_t dummy;

  NodePage(uint8_t cur_level, GlobalAddress remote)
      : header(cur_level, remote) {
    if (cur_level == 0) {
      is_leaf = true;
    }
  }

  bool isFull() { return header.count == kInternalCardinality; }

  unsigned lowerBound(Key k) const {
    if (k < keys[0]) {
      return LeftMostIdx;
    }
    unsigned lower = 0;
    unsigned upper = header.count - 1;
    do {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (k < keys[mid]) {
        upper = mid;
      } else if (k > keys[mid]) {
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  // TODO:adjust all
  int findIdx(uint64_t target_addr) {
    int end = header.count;
    for (int i = 0; i < end; i++) {
      if (values[i] == target_addr) {
        return i;
      }
    }

    if (left_ptr.val == target_addr) {
      return LeftMostIdx;
    }

    return -1;
  }

  void set_consistent() {
    this->crc =
        CityHash32((char*)&front_version, (&rear_version) - (&front_version));
  }

  bool check_consistent() const {
    bool succ = true;
    auto cal_crc =
        CityHash32((char*)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
    return succ;
  }

  bool isLocked(uint64_t version) { return header.isLocked(); }

  bool isLocked() { return header.isLocked(); }

  uint64_t readLockOrRestart(bool& needRestart) {
    return header.readLockOrRestart(needRestart);
  }

  void checkOrRestart(uint64_t startRead, bool& needRestart) const {
    header.checkOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool& needRestart) const {
    header.readUnlockOrRestart(startRead, needRestart);
  }

  void writeLockOrRestart(bool& needRestart) {
    header.writeLockOrRestart(needRestart);
  }

  void upgradeToWriteLockOrRestart(uint64_t& version, bool& needRestart) {
    header.upgradeToWriteLockOrRestart(version, needRestart);
  }

  void IOUpgradeToWriteLockOrRestart(bool& needRestart) {
    header.IOUpgradeToWriteLockOrRestart(needRestart);
  }

  void IOLockOrRestart(bool& needRestart) {
    header.IOLockOrRestart(needRestart);
  }

  void upgradeToIOLockOrRestart(uint64_t& version, bool& needRestart) {
    header.upgradeToIOLockOrRestart(version, needRestart);
  }

  void IOUnlock() { header.IOUnlock(); }

  void writeUnlock() { header.writeUnlock(); }

  // void writeUnlockWithIOLock() { typeVersionLockObsolete.fetch_add(0b101); }

  void setLockState() { header.setLockState(); }

  bool isObsolete(uint64_t version) { return isObsolete(version); }

  bool isIO(uint64_t version) { return isIO(version); }

  void writeUnlockObsolete() { header.writeUnlockObsolete(); }

  // RPC
  bool find(Key k, Value& v) {
    auto pos = lowerBound(k);
    bool success = false;
    if ((pos < header.count) && keys[pos] == k) {
      success = true;
      v = values[pos];
    }
    return success;
  }
};

constexpr int keyNum = kInternalCardinality;  // Maximum number of keys per node

class BTreeNode {
 public:
  BTreeNode* leftmost_ptr;
  BTreeNode * next_leaf =nullptr;
  uint64_t keys[keyNum];  // Stores up to keyNum keys
  uint64_t
      children[keyNum];  // Internal nodes: child pointers; Leaf nodes: values
  int numKeys;           // Number of valid keys in the node
  int degree;            // Minimum degree
  bool isLeaf;           // Is true when node is a leaf
  
  int my_btree_node_id;

  static int node_count;
  static BTreeNode* first_ten[10];

  BTreeNode(int degree, bool isLeaf) {
    this->degree = degree;
    this->isLeaf = isLeaf;
    this->numKeys = 0;
    this->leftmost_ptr = nullptr;
    my_btree_node_id = node_count;
    first_ten[node_count] = this;
    node_count++;

    // Initialize keys and children with 0
    for (int i = 0; i < keyNum; i++) {
      keys[i] = 0;
      children[i] = 0;
    }
  }
  void insertNonFull(uint64_t key, uint64_t value) {
    int i = numKeys - 1;

    if (isLeaf) {
      // Shift keys and values
      for (int s = 0; s < numKeys; s++) {
        if (keys[s] == key) {
          keys[s] = value;
          return;
        }
      }

      while (i >= 0 && keys[i] > key) {
        keys[i + 1] = keys[i];
        children[i + 1] = children[i];
        i--;
      }
      keys[i + 1] = key;
      children[i + 1] = value;  // Store the value
      numKeys++;
    } else {
      // Internal node: find where the key should go
      if (key < keys[0]) {
        if (leftmost_ptr->numKeys == keyNum) {
          splitChild(-1, leftmost_ptr);  // Split the leftmost child
        }
        if (keys[0] <= key) {
          ((BTreeNode*)children[0])->insertNonFull(key, value);
        } else {
          leftmost_ptr->insertNonFull(key, value);
        }
      } else {
        while (i > 0 && keys[i] > key) {
          i--;
        }
        BTreeNode* child = (BTreeNode*)children[i];
        if (child->numKeys == keyNum) {
          splitChild(i, child);
          // for (uint64_t op =0; op < numKeys;op++) {
          //   std::cout << "key i " << op << " " << keys[op] <<std::endl;
          // }
          if (keys[i + 1] <= key) {
            i++;
          }
        }
        ((BTreeNode*)children[i])->insertNonFull(key, value);
      }
    }
  }
  void splitChild(int i, BTreeNode* y) {
    // Create a new node to store (degree - 1) keys of y
    // std::cout << "split i" << i <<std::endl;
    // for (uint64_t op = 0; op < numKeys; op++) {
    //   std::cout << "2key i " << op << " " << keys[op] << std::endl;
    // }
    BTreeNode* z = new BTreeNode(y->degree, y->isLeaf);
    z->numKeys = degree - 1;

    int y_next_num = y->numKeys - z->numKeys;
    // Copy the second half of y's keys to z
    for (int j = 0; j < degree - 1; j++) {
      z->keys[j] = y->keys[j + y_next_num];
      z->children[j] = y->children[j + y_next_num];
    }
    y->numKeys -= degree - 1;
    // cout << "number in y: " << y->numKeys << std::endl;

    // cout << "number in node: " << numKeys << std::endl;
    // for (int j = numKeys ;  j >= i + 1; j--) {
    //   children[j + 1] = children[j];
    //   keys[j + 1] = keys[j];
    // }
    for (int j = numKeys - 1; j >= i + 1; j--) {
      children[j + 1] = children[j];
      keys[j + 1] = keys[j];
    }
    // cout << "number in node: "<<numKeys<<std::endl;
    // for (uint64_t op = 0; op < numKeys; op++) {
    //   std::cout << "2key i " << op << " " << keys[op] << std::endl;
    // }

    // if (i == -1) {
    //   children[0] = (uint64_t)z;
    //   keys[0] = z->keys[0];
    // } else {
    //   for (int j = numKeys; j >= i + 1; j--) {
    //     children[j + 1] = children[j];
    //     keys[j + 1] = keys[j];
    //   }

    children[i + 1] = (uint64_t)z;
    keys[i + 1] = z->keys[0];


    // for (uint64_t op = 0; op < numKeys; op++) {
    //   std::cout << "1key i " << op << " " << keys[op] << std::endl;
    // }

    // std::cout << "split key: " <<z->keys[0] << std::endl;

    // Insert the middle key of y into the current node
    numKeys++;

    if (y->isLeaf) {
      z->next_leaf = y->next_leaf;
      y->next_leaf = z;
    }
  }

  GlobalAddress send_to_remote_internal(DSM* dsm, const GlobalAddress& mem_base,
                                        GlobalAddress* gas,
                                        const GlobalAddress& left_ga, int level,
                                        uint64_t min_lim, uint64_t max_lim) {
    GlobalAddress my_address;
    my_address.nodeID = mem_base.nodeID;
    my_address.offset = mem_base.offset + kPageSize * my_btree_node_id;

    auto remote_page_buffer = dsm->get_rbuf(0).get_page_buffer();
    NodePage* page_ptr =
        new (remote_page_buffer) NodePage((uint8_t)level, my_address);

    NodeHeader* header = &page_ptr->header;
    // set header
    header->count = numKeys;
    header->max_limit_ = max_lim;
    header->min_limit_ = min_lim;
    // set value
    for (int i = 0; i < numKeys; i++) {
      page_ptr->keys[i] = keys[i];
      page_ptr->values[i] = gas[i].val;
    }
    page_ptr->left_ptr = left_ga;
    page_ptr->is_leaf = false;
    page_ptr->set_consistent();

    // send to remote
    dsm->write_sync(remote_page_buffer, my_address, kPageSize);
    return my_address;
  }

  GlobalAddress send_to_remote_leaf(DSM* dsm, const GlobalAddress& mem_base,
                               int level,
                                    uint64_t min_lim, uint64_t max_lim) {
    GlobalAddress my_address;
    my_address.nodeID = mem_base.nodeID;
    my_address.offset = mem_base.offset + kPageSize * my_btree_node_id;
    auto remote_page_buffer = dsm->get_rbuf(0).get_page_buffer();
    NodePage* page_ptr = new (remote_page_buffer) NodePage(0, my_address);

    NodeHeader* header = &page_ptr->header;
    // set header
    header->count = numKeys;
    header->max_limit_ = max_lim;
    header->min_limit_ = min_lim;

    for (int i = 0; i < numKeys; i++) {
      page_ptr->keys[i] = keys[i];
      page_ptr->values[i] = children[i];
    }
    if (next_leaf == nullptr) {
      page_ptr->left_ptr = GlobalAddress::Null();
    } else {
      int next_btree_leaf_node_id = next_leaf->my_btree_node_id;
      GlobalAddress next_address;
      next_address.nodeID = mem_base.nodeID;
      next_address.offset = mem_base.offset + kPageSize * next_btree_leaf_node_id;
      page_ptr->left_ptr = next_address;
    }
    page_ptr->is_leaf = true;
    page_ptr->set_consistent();

    // send to remote
    dsm->write_sync(remote_page_buffer, my_address, kPageSize);
    return my_address;
  }

  GlobalAddress to_remote(DSM* dsm, const GlobalAddress& mem_base, int level,
                          uint64_t min_lim, uint64_t max_lim) {
    if (!isLeaf) {
      GlobalAddress ga_buffer[keyNum];
      GlobalAddress left_ga = GlobalAddress::Null();
      if (leftmost_ptr != nullptr) {
        GlobalAddress left_ga =
            leftmost_ptr->to_remote(dsm, mem_base, level - 1, min_lim, keys[0]);
        // leftmost_ptr->traverse(level + 1);
      }
      for (int i = 0; i < numKeys; i++) {
        BTreeNode* child = reinterpret_cast<BTreeNode*>(children[i]);
        if (child != nullptr) {
          uint64_t next_level_max_lim =
              (i == numKeys - 1) ? max_lim : keys[i + 1];
          ga_buffer[i] = child->to_remote(
              dsm, mem_base, level - 1, keys[i],
              next_level_max_lim);  // Increment level for child nodes
        }
      }
      return send_to_remote_internal(dsm, mem_base, ga_buffer, left_ga, level, min_lim, max_lim);
    } else {
      assert (level == 0);
      return send_to_remote_leaf(dsm, mem_base, level, min_lim, max_lim);
    }
  }

  void traverse(int level) {
    std::cout << "Level " << level << " (" << (isLeaf ? "Leaf" : "Internal")
              << ") - Keys: ";

    for (int i = 0; i < numKeys; i++) {
      std::cout << keys[i] << " ";
    }
    if (isLeaf) {
      std::cout << " - Values: ";
      for (int i = 0; i < numKeys; i++) {
        std::cout << children[i] << " ";  // Print values at the leaf nodes
      }
    }
    std::cout << std::endl;

    // If it's not a leaf, recursively print the children
    if (!isLeaf) {
      if (leftmost_ptr != nullptr) {
        leftmost_ptr->traverse(level + 1);
      }
      for (int i = 0; i < numKeys; i++) {
        BTreeNode* child = reinterpret_cast<BTreeNode*>(children[i]);
        if (child != nullptr) {
          child->traverse(level + 1);  // Increment level for child nodes
        }
      }
    }
  }  // Modified to accept a "level" parameter for depth traversal
  BTreeNode* search(uint64_t key);

 private:
  int maxKeys() { return keyNum; }  // Maximum number of keys (fixed size array)
  int minKeys() { return degree - 1; }  // Minimum number of keys
};

class BTree {
 public:
  BTreeNode* root;
  int height;

  // For Debug
  BTreeNode* first_ten[10]{0};
  int cur_ten = 0;

  int degree;

  BTree(int degree) {
    root = nullptr;
    this->degree = degree;
  }

  void traverse() {
    if (root != nullptr) root->traverse(0);  // Start from level 0 (root)
  }

  BTreeNode* search(uint64_t key) {
    return (root == nullptr) ? nullptr : root->search(key);
  }

  void insert(uint64_t key, uint64_t value) {
    if (root == nullptr) {
      root = new BTreeNode(degree, true);
      root->keys[0] = key;
      root->children[0] = value;
      root->numKeys = 1;
      height = 0;
    } else {
      // std::cout << "have root"<<std::endl;
      // std::cout <<"root num key" << root->numKeys <<std::endl;
      if (root->numKeys == keyNum) {
        // std::cout << "aplit root" <<std::endl;
        BTreeNode* newNode = new BTreeNode(degree, false);
        newNode->leftmost_ptr = root;
        newNode->splitChild(-1, root);

        int i = 0;
        if (newNode->keys[0] < key) {
          ((BTreeNode*)newNode->children[i])->insertNonFull(key, value);
        } else {
          newNode->leftmost_ptr->insertNonFull(key, value);
        }
        root = newNode;
        height += 1;
      } else {
        root->insertNonFull(key, value);
      }
    }
  }
};

int BTreeNode::node_count = 0;
BTreeNode* BTreeNode::first_ten[10]{0};

}  // namespace XMD
