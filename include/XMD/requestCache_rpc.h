#pragma once

#include "../DSM.h"
#include "../GlobalAddress.h"
#include "../third_party/libcuckoo/cuckoohash_map.hh"
#include "Common.h"
#include <cstdint>

namespace XMD {
constexpr uint64_t defaultSlot = 4;

/* GlobalAddress
struct GlobalAddress {
   union {
    struct {
      uint64_t nodeID : 16;
      uint64_t offset : 48;
    };
    uint64_t val;
  };

};
 */

struct HashSlot {
  Key k;   // u64
  Value v; // u64
  TS ts;   // u64
} __attribute__((packed));

struct HashBucket {
  HashSlot slots[defaultSlot];
} __attribute__((packed));

constexpr uint64_t perHashCacheSize = define::kMemorySideHashSize / 4;
constexpr uint64_t kHashBucketNum = perHashCacheSize / sizeof(HashBucket);

class ComputeSideHash {
  ComputeSideHash(DSM *dsm):dsm_(dsm) {
    my_node_id_ = dsm_->getMyNodeID();
    int num_cnode = dsm_->getComputeNum();
    remote_ht_start_.nodeID = dsm_->getComputeNum();
    remote_ht_start_.offset = perHashCacheSize * my_node_id_;
    remote_ht_size_ = perHashCacheSize;
  }
  
  void insert(const KVTS & kvts) {
    u64 i1, i2;
    get_two_bucket_pos(kvts.k, i1, i2);
    assert(dsm_->is_register());
    // insert to local

    //synchro to remote

    // if fail call cuckoo kick rpc to remote
  }

  void find(Key k, Value &v) {
    u64 i1, i2;
    get_two_bucket_pos(k, i1, i2);
    assert(dsm_->is_register());


    
  }

private:
  using partial_t = uint8_t;

  static uint64_t reserve_calc(const uint64_t n) {
    const uint64_t buckets = (n + defaultSlot - 1) / defaultSlot;
    uint64_t blog2;
    for (blog2 = 0; (uint64_t(1) << blog2) < buckets; ++blog2)
      ;
    assert(n <= buckets * defaultSlot && buckets <= hashsize(blog2));
    return blog2;
  }

  static inline uint64_t hashsize(const uint64_t hp) {
    return u64(1) << hp;
  }

  static inline uint64_t hashmask(const uint64_t hp) {
    return hashsize(hp) - 1;
  }

  static partial_t partial_key(const u64 hash) {
    const uint64_t hash_64bit = hash;
    const uint32_t hash_32bit = (static_cast<uint32_t>(hash_64bit) ^
                                 static_cast<uint32_t>(hash_64bit >> 32));
    const uint16_t hash_16bit = (static_cast<uint16_t>(hash_32bit) ^
                                 static_cast<uint16_t>(hash_32bit >> 16));
    const uint8_t hash_8bit = (static_cast<uint8_t>(hash_16bit) ^
                               static_cast<uint8_t>(hash_16bit >> 8));
    return hash_8bit;
  }

  struct hash_value {
    uint64_t hash;
    partial_t partial;
  };

  hash_value hashed_key(const u64 &key) const {
    const uint64_t hash = std::hash<u64>()(key);
    return {hash, partial_key(hash)};
  }

  uint64_t hashed_key_only_hash(const u64 &key) const {
    return std::hash<u64>()(key);
  }

  static inline uint64_t index_hash(const uint64_t hp, const uint64_t hv) {
    return hv & hashmask(hp);
  }

  static inline uint64_t alt_index(const uint64_t hp, const partial_t partial,
                                    const uint64_t index) {
    // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
    // hash constant from 64-bit MurmurHash2
    const uint64_t nonzero_tag = static_cast<uint64_t>(partial) + 1;
    return (index ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & hashmask(hp);
  }

  inline void get_two_bucket_pos(Key key, u64 &i1, u64 &i2) {
    const hash_value hv = hashed_key(key);
    const uint64_t hp = hash_power_;
    i1 = index_hash(hp, hv.hash);
    i2 = alt_index(hp, hv.partial, i1);
  }

  using counter_type = int64_t;
  LIBCUCKOO_SQUELCH_PADDING_WARNING
  class LIBCUCKOO_ALIGNAS(64) spinlock {
  public:
    spinlock() : elem_counter_(0), is_migrated_(true) { lock_.clear(); }

    spinlock(const spinlock &other) noexcept
        : elem_counter_(other.elem_counter()),
          is_migrated_(other.is_migrated()) {
      lock_.clear();
    }

    spinlock &operator=(const spinlock &other) noexcept {
      elem_counter() = other.elem_counter();
      is_migrated() = other.is_migrated();
      return *this;
    }

    void lock() noexcept {
      while (lock_.test_and_set(std::memory_order_acq_rel))
        ;
    }

    void unlock() noexcept { lock_.clear(std::memory_order_release); }

    bool try_lock() noexcept {
      return !lock_.test_and_set(std::memory_order_acq_rel);
    }

    counter_type &elem_counter() noexcept { return elem_counter_; }
    counter_type elem_counter() const noexcept { return elem_counter_; }

    bool &is_migrated() noexcept { return is_migrated_; }
    bool is_migrated() const noexcept { return is_migrated_; }

  private:
    std::atomic_flag lock_;
    counter_type elem_counter_;
    bool is_migrated_;
  };

  spinlock locks_[kHashBucketNum];
  u64 hash_power_ = reserve_calc(perHashCacheSize);
  DSM *dsm_;
  uint64_t my_node_id_;
  GlobalAddress remote_ht_start_;
  uint64_t remote_ht_size_;
};

class MRequestCache {

public:
  MRequestCache(GlobalAddress start, uint64_t range, int num_cnode) {
    for (int i = 0; i < num_cnode; i++) {
      GlobalAddress addr = start;
      addr.offset += (range / num_cnode) * i;
      buckets_[i] = addr;
    }
  }

  int lookup(int node_id, Key key, Value &v) {}

  int insert(int node_id, Key key, TS ts, Value v) {}

  GlobalAddress buckets_[MAX_MACHINE];
  DSM *dsm_;
};
// // -1 means lookup failure, 0 means next global_address_ptr, 1 means find
// value
// // result, 2 means find nothing
// int lookup(GlobalAddress root, uint64_t dsm_base, Key k, Value &v_result,
//            GlobalAddress &g_result) {
//   auto node_id = root.nodeID;
//   auto node = root;
//   NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
//   while (mem_node->type == PageType::BTreeInner) {
//     auto inner = static_cast<BTreeInner<Key> *>(mem_node);
//     node = inner->children[inner->lowerBound(k)];
//     if (node.nodeID != node_id) {
//       g_result = node;
//       return 0;
//     } else {
//       mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
//     }
//   }

//   BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value>
//   *>(mem_node); if (!leaf->rangeValid(k)) {
//     return -1;
//   }

//   unsigned pos = leaf->lowerBound(k);
//   int ret = 2;
//   if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
//     ret = 1;
//     v_result = leaf->data[pos].second;
//   }

//   return ret;
// }

// // -1 means update failure because enterring wrong leaf node, 0 means failure
// // because not enter into a leaf node, 1 means update value succeeds, 2 means
// // update nothing
// int update(GlobalAddress &root, uint64_t dsm_base, Key k, Value v_result) {
//   auto node_id = root.nodeID;
//   auto node = root;
//   NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
//   while (mem_node->type == PageType::BTreeInner) {
//     auto inner = static_cast<BTreeInner<Key> *>(mem_node);
//     node = inner->children[inner->lowerBound(k)];
//     if (node.nodeID != node_id) {
//       return 0;
//     } else {
//       mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
//     }
//   }

//   BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value>
//   *>(mem_node); if (!leaf->rangeValid(k)) {
//     return -1;
//   }

//   unsigned pos = leaf->lowerBound(k);
//   int ret = 2;
//   if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
//     ret = 1;
//     leaf->data[pos].second = v_result;
//     root = leaf->remote_address;
//   }

//   return ret;
// }

// // -1 means insert failure because enterring wrong leaf node (-1 also means
// it
// // needs to SMO), 0 means failure because not enter into a leaf node, 1 means
// // insert value succeeds, 2 means update a existing value;
// int insert(GlobalAddress &root, uint64_t dsm_base, Key k, Value v_result) {
//   auto node_id = root.nodeID;
//   auto node = root;
//   NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
//   while (mem_node->type == PageType::BTreeInner) {
//     auto inner = static_cast<BTreeInner<Key> *>(mem_node);
//     node = inner->children[inner->lowerBound(k)];
//     if (node.nodeID != node_id) {
//       return 0;
//     } else {
//       mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
//     }
//   }

//   BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value>
//   *>(mem_node); if (!leaf->rangeValid(k) || (leaf->count ==
//   leaf->maxEntries)) {
//     return -1;
//   }

//   root = leaf->remote_address;
//   bool insert_success = leaf->insert(k, v_result);
//   if (insert_success) {
//     return 1;
//   }

//   return 2;
// }

// // -1 means remove failure because enterring wrong leaf node, 0 means failure
// // because not enter into a leaf node, 1 means remove value succeeds, 2 means
// // remove nothing
// int remove(GlobalAddress &root, uint64_t dsm_base, Key k) {
//   auto node_id = root.nodeID;
//   auto node = root;
//   NodeBase *mem_node = reinterpret_cast<NodeBase *>(dsm_base + root.offset);
//   while (mem_node->type == PageType::BTreeInner) {
//     auto inner = static_cast<BTreeInner<Key> *>(mem_node);
//     node = inner->children[inner->lowerBound(k)];
//     if (node.nodeID != node_id) {
//       return 0;
//     } else {
//       mem_node = reinterpret_cast<NodeBase *>(dsm_base + node.offset);
//     }
//   }

//   BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value>
//   *>(mem_node); if (!leaf->rangeValid(k)) {
//     return -1;
//   }

//   auto flag = leaf->remove(k);
//   int ret = flag ? 1 : 2;
//   root = leaf->remote_address;
//   return ret;
// }

} // namespace XMD