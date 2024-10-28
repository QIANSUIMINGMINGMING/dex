#pragma once

#include <atomic>
#include <mutex>
#include <queue>
#include <random>
#include <shared_mutex>
#include <thread>

#include "Common.h"

namespace XMD {
template <typename T>
class MonotonicBufferRing {
 public:
  MonotonicBufferRing(size_t size) : size(size), buffer(size * sizeof(T)) {}

  T *alloc(size_t &start_offset) {
    size_t pos = offset.fetch_add(1);
    while (!have_space(pos, 1));
    start_offset = pos % size;
    return &buffer[pos % size];
  }

  T *alloc(size_t n, size_t &start_offset) {
    size_t pos = offset.fetch_add(n);
    while (!have_space(pos, n));
    start_offset = pos % size;
    return &buffer[pos % size];
  }

  size_t check_distance(size_t start_offset, size_t &cur_offset) {
    cur_offset = offset.load() % size;
    return RING_SUB(cur_offset, start_offset, size);
  }

  void release(size_t start_offset) {
  restart:
    bool need_restart = false;
    cutil::ull_t cur_v = cutil::read_lock_or_restart(start_mutex, need_restart);
    if (need_restart) {
      goto restart;
    }
    cutil::upgrade_to_write_lock_or_restart(start_mutex, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    start += RING_SUB(start_offset, start, size);
    cutil::write_unlock(start_mutex);
  }

  // operator[]
  T &operator[](size_t i) { return buffer[i % size]; }
  inline size_t getOffset() { return offset.load() % size; }

 private:
  inline bool have_space(size_t old, size_t n) {
  restart:
    bool need_restart = false;
    cutil::ull_t cur_v = cutil::read_lock_or_restart(start_mutex, need_restart);
    if (need_restart) {
      goto restart;
    }
    bool ret = old + n - start < size;
    cutil::read_unlock_or_restart(start_mutex, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    return ret;
  }

  size_t size;
  HugePages<T> buffer;
  volatile size_t start{0};
  std::atomic<size_t> offset{0};
  std::atomic<cutil::ull_t> start_mutex;
};

// inline skiplist relevant
constexpr int kMaxLevel = 8;
constexpr int kMaxSkipListData = (1 << 16) - 1;
 
struct SkipListNode {
  KVTS kvts;
  uint16_t level;
  uint16_t next_ptrs[kMaxLevel];  // ptrs are relative distance of the start of
                                  // skiplistNode
} __attribute__((packed));
}  // namespace XMD
