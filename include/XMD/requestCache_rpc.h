#pragma once

#include <cstdint>
#include <cstdlib>
#include <iostream>

#include "../Common.h"
#include "../DSM.h"
#include "../GlobalAddress.h"
#include "../third_party/libcuckoo/cuckoohash_map.hh"

namespace XMD {
constexpr uint64_t defaultSlot = 4;

struct HashSlot {
  Key k{0};    // u64
  Value v{0};  // u64
  TS ts{0};    // u64
} __attribute__((packed));

struct HashBucket {
  HashSlot slots[defaultSlot];
} __attribute__((packed));

constexpr uint64_t size_per_bucket = sizeof(HashSlot) * defaultSlot;

constexpr uint64_t perHashCacheSize = define::kMemorySideHashSize / 4;
constexpr uint64_t kHashBucketNum = perHashCacheSize / sizeof(HashBucket);

class ComputeSideHash {
 public:
  ComputeSideHash(DSM *dsm) : dsm_(dsm) {
    my_node_id_ = dsm_->getMyNodeID();
    int num_cnode = dsm_->getComputeNum();
    remote_ht_start_.nodeID = dsm_->getComputeNum();
    remote_ht_start_.offset = perHashCacheSize * my_node_id_;
    remote_ht_size_ = perHashCacheSize;
  }

  void insert(const KVTS &kvts, TS oldest_ts) {
    u64 i1, i2;
    get_two_bucket_pos(kvts.k, i1, i2);
    assert(dsm_->is_register());
    auto hash_buffer = dsm_->get_rbuf(0).get_hash_buffer();
    assert(hash_buffer != nullptr);
    // insert to local
    locks_[i1].lock();
    GlobalAddress remote_bucket;
    remote_bucket.nodeID = remote_ht_start_.nodeID;
    remote_bucket.offset = remote_ht_start_.offset + i1 * size_per_bucket;
    dsm_->read_sync(hash_buffer, remote_bucket, size_per_bucket);
    HashBucket *hash_bucket = reinterpret_cast<HashBucket *>(hash_buffer);
    int insert_pos = defaultSlot;
    for (int i = 0; i < defaultSlot; i++) {
      if (hash_bucket->slots[i].k == kvts.k) {
        insert_pos = i;
        break;
      }
      if (hash_bucket->slots[i].k == 0 ||
          hash_bucket->slots[i].ts < oldest_ts) {
        insert_pos = i;
      }
    }
    if (insert_pos != defaultSlot) {
      HashSlot *slot = &hash_bucket->slots[insert_pos];
      slot->k = kvts.k;
      slot->ts = kvts.ts;
      slot->v = kvts.v;
      GlobalAddress remote_slot;
      remote_slot.nodeID = remote_ht_start_.nodeID;
      remote_slot.offset = remote_ht_start_.offset + i1 * size_per_bucket +
                           insert_pos * sizeof(HashSlot);
      dsm_->write_sync((char *)slot, remote_slot, sizeof(HashSlot));
      locks_[i1].unlock();
      return;
    }

    locks_[i2].lock();

    // if fail call cuckoo kick rpc to remote

    int kick_slot = rand() % defaultSlot;
    Key kick_key = hash_bucket->slots[kick_slot].k;
    u64 k1, k2;
    get_two_bucket_pos(kick_key, k1, k2);

    u64 lock_pos_k;
    if (k1 != i1 && k1 != i2) {
      lock_pos_k = k1;
      assert(k2 == i1 || k2 == i2);
    } else {
      assert(k2 != i1 && k2 != i2);
      lock_pos_k = k2;
    }

    locks_[lock_pos_k].lock();
    dsm_->rpc_cuckoo_kick(remote_ht_start_, kvts, oldest_ts, kick_slot);

    locks_[lock_pos_k].unlock();
    locks_[i1].unlock();
    locks_[i2].unlock();
  }

  bool find(Key k, Value &v) {
    u64 i1, i2;
    get_two_bucket_pos(k, i1, i2);
    assert(dsm_->is_register());

    auto hash_buffer = dsm_->get_rbuf(0).get_hash_buffer();
    locks_[i1].lock();
    GlobalAddress remote_bucket;
    remote_bucket.nodeID = remote_ht_start_.nodeID;
    remote_bucket.offset = remote_ht_start_.offset + i1 * size_per_bucket;
    dsm_->read_sync(hash_buffer, remote_bucket, size_per_bucket);
    HashBucket *hash_bucket = reinterpret_cast<HashBucket *>(hash_buffer);
    for (int i = 0; i < defaultSlot; i++) {
      if (hash_bucket->slots[i].k == k) {
        v = hash_bucket->slots[i].v;
        locks_[i1].unlock();
        return true;
      }
    }
    locks_[i1].unlock();
    locks_[i2].lock();
    GlobalAddress remote_bucket2;
    remote_bucket2.nodeID = remote_ht_start_.nodeID;
    remote_bucket2.offset = remote_ht_start_.offset + i2 * size_per_bucket;
    dsm_->read_sync(hash_buffer, remote_bucket2,
                    size_per_bucket);
    hash_bucket = reinterpret_cast<HashBucket *>(hash_buffer);
    for (int i = 0; i < defaultSlot; i++) {
      if (hash_bucket->slots[i].k == k) {
        v = hash_bucket->slots[i].v;
        locks_[i2].unlock();
        return true;
      }
    }
    locks_[i2].unlock();
    return false;
  }

 private:
  using partial_t = uint8_t;

  static uint64_t reserve_calc(const uint64_t n) {
    const uint64_t buckets = (n + defaultSlot - 1) / defaultSlot;
    uint64_t blog2;
    for (blog2 = 0; (uint64_t(1) << blog2) < buckets; ++blog2);
    assert(n <= buckets * defaultSlot && buckets <= hashsize(blog2));
    return blog2;
  }

  static inline uint64_t hashsize(const uint64_t hp) { return u64(1) << hp; }

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
      while (lock_.test_and_set(std::memory_order_acq_rel));
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
  MRequestCache(GlobalAddress start, char *dsm_base, uint64_t range,
                int num_cnode) {
    for (int i = 0; i < num_cnode; i++) {
      GlobalAddress addr = start;
      addr.offset += perHashCacheSize * i;
      HashBucket *bucket_ptr =
          reinterpret_cast<HashBucket *>(dsm_base + addr.offset);
      buckets_[i] = bucket_ptr;
    }
  }

  int cuckoo_kick(uint16_t cnode_id, Key k, Value v, TS ts, TS oldest_TS,
                  int kick_slot) {
    auto addr = buckets_[cnode_id];
    u64 i1, i2;
    get_two_bucket_pos(k, i1, i2);

    HashBucket *b1 = addr + i1;
    HashBucket *b2 = addr + i2;

    int b1_slot;

    if (get_slot(b1, b1_slot, k, oldest_TS)) {
      b1->slots[b1_slot].k = k;
      b1->slots[b1_slot].ts = ts;
      b1->slots[b1_slot].v = v;
      return 0;
    }

    int b2_slot;
    if (get_slot(b2, b2_slot, k, oldest_TS)) {
      b2->slots[b2_slot].k = k;
      b2->slots[b2_slot].ts = ts;
      b2->slots[b2_slot].v = v;
      return 0;
    }

    // cuckoo kick
    u64 k1, k2;
    Key kick_k = b1->slots[kick_slot].k;

    HashSlot *kick_slot_val = &b1->slots[kick_slot];

    get_two_bucket_pos(kick_k, k1, k2);

    u64 kick_to_k;

    if (k1 != i1 && k1 != i2) {
      kick_to_k = k1;
      assert(k2 == i1 || k2 == i2);
    } else {
      assert(k2 != i1 && k2 != i2);
      kick_to_k = k2;
    }

    HashBucket *kick_b = addr + kick_to_k;

    int to_slot;
    if (get_slot(kick_b, to_slot, kick_k, oldest_TS)) {
      kick_b->slots[to_slot].k = kick_k;
      kick_b->slots[to_slot].ts = kick_slot_val->ts;
      kick_b->slots[to_slot].v = kick_slot_val->v;

      b1->slots[kick_slot].k = k;
      b1->slots[kick_slot].ts = ts;
      b1->slots[kick_slot].v = v;
      return 0;
    } else {
      std::cout << "Hash Size should be larger?" << std::endl;
      assert(false);
    }
    return 1;
  }

 private:
  inline bool get_slot(HashBucket *b, int &slot, Key k, TS oldest_TS) {
    int insert_pos = defaultSlot;
    for (int i = 0; i < defaultSlot; i++) {
      if (b->slots[i].k == k) {
        insert_pos = i;
        break;
      }
      if (b->slots[i].k == 0 || b->slots[i].ts < oldest_TS) {
        insert_pos = i;
      }
    }
    if (insert_pos == defaultSlot) {
      return false;
    } else {
      slot = insert_pos;
      return true;
    }
  }

  using partial_t = uint8_t;

  static uint64_t reserve_calc(const uint64_t n) {
    const uint64_t buckets = (n + defaultSlot - 1) / defaultSlot;
    uint64_t blog2;
    for (blog2 = 0; (uint64_t(1) << blog2) < buckets; ++blog2);
    assert(n <= buckets * defaultSlot && buckets <= hashsize(blog2));
    return blog2;
  }

  static inline uint64_t hashsize(const uint64_t hp) { return u64(1) << hp; }

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

  HashBucket *buckets_[MAX_MACHINE];
  u64 hash_power_ = reserve_calc(perHashCacheSize);
};

}  // namespace XMD